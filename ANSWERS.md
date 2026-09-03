# ANSWERS — Day 28 Track 2

> Làm cá nhân. Nhánh `ca-nhan-duong`. Máy làm bài: Windows 11, 8 CPU, ~8 GB đĩa
> trống → `preflight` cho `browser-fallback`, nên Bước 1–6 chạy tại máy, Bước
> 7–10 cần môi trường stack do giảng viên cấp (đánh dấu `UNVERIFIED` ở dưới cho
> tới khi chạy được).

## 1. Trạng thái checkpoint

| Checkpoint | Lệnh | Kết quả |
|---|---|---|
| Fast suite | `uv run pytest starter-tests tests -q` | 87 passed |
| Starter — Part A | `uv run pytest starter-tests/test_integration_tasks.py -k event_headers -q` | 1 passed, 3 deselected |
| Starter — Part B | `uv run pytest tests/test_delta_merge_idempotency.py -q` | 22 passed |
| Lint | `uv run ruff check .` | All checks passed |
| Matrix | `uv run python scripts/verify_matrix.py` | 245 checks passed |
| Portability | `uv run python scripts/check_portability.py` | OK |
| Manifests | `uv run python scripts/validate_manifests.py` | passed |
| Compose config | `docker compose --env-file ports.template [--profile full] config --quiet` | exit 0 |
| Live stack (Bước 7–10) | — | `UNVERIFIED` — chưa có môi trường stack |

Chỉ sửa một file: `src/lab28_platform/integration_tasks.py`. Không sửa test.

## 2. Bốn boundary — lựa chọn kỹ thuật và trade-off

### A. `event_headers` — IP01 + IP10 (trace + idempotency qua Kafka)

- **Chọn:** luôn phát header `idempotency-key` (bytes); chỉ thêm `traceparent`
  khi có trace đang hoạt động. Giá trị lấy từ tham số, không hard-code.
- **Vì sao:** W3C `traceparent` có định dạng chặt (`00-<32 hex>-<16 hex>-<2 hex>`).
  Gửi chuỗi rỗng khi chưa có trace tạo ra một header hợp lệ về mặt "có mặt" nhưng
  sai định dạng — consumer phía sau sẽ hoặc reject, hoặc tệ hơn là parse ra một
  trace ID rác làm gãy việc nối span. Bỏ hẳn header khi không có trace là cách
  duy nhất giữ IP10 (một trace xuyên suốt) trung thực.
- **Trade-off:** header trả về là `list` (mutable) chứ không phải `tuple`, vì
  `event_bus.publish` còn `append` thêm `schema_version` sau khi gọi hàm này.
- **Kiểm chứng nhanh:** so hai phía producer/consumer cùng một trace ID (không so
  span ID — span ID khác nhau giữa các hop là đúng).

### B. `dedupe_latest` — IP03 (nguồn MERGE an toàn khi replay)

- **Chọn:** duyệt input **đúng một lần**, giữ trong `dict` một event cho mỗi
  `idempotency_key`, so sánh khóa sắp thứ tự `(occurred_at, event_id)` và giữ bản
  lớn nhất; cuối cùng trả danh sách theo `sorted(idempotency_key)`.
- **Vì sao khóa `(occurred_at, event_id)` chứ không chỉ `occurred_at`:** Kafka chỉ
  đảm bảo thứ tự *trong một partition*, không phải trong một batch. Hai bản ghi
  trùng timestamp (một correction gửi lại) mà chỉ so `occurred_at` sẽ cho kết quả
  phụ thuộc thứ tự Kafka giao — không tái lập. Thêm `event_id` làm tie-breaker
  khiến kết quả chỉ phụ thuộc *nội dung* batch.
- **Vì sao sắp xếp đầu ra theo key:** Delta MERGE fail ngay nếu source có hai
  dòng khớp cùng một dòng target; sắp xếp ổn định giúp mỗi lần chạy cho cùng thứ
  tự, dễ so sánh trong test và trong demo replay.
- **Vì sao chọn `idempotency_key` làm merge key** (không phải `event_id` hay
  timestamp): `event_id` khác nhau mỗi lần API tạo event, nên replay bằng
  `event_id` sẽ chèn thêm dòng chỉ khác metadata — đúng thứ IP03 tồn tại để loại
  bỏ. `idempotency_key` là "cùng một sự thật".
- **Trade-off:** giữ toàn bộ batch trong bộ nhớ (chấp nhận được cho batch mức
  Airflow task); một pipeline stream thực sự sẽ cần windowed dedupe + state store.

### C. `feast_online_request` — IP04 (contract API ↔ Feast)

- **Chọn:** `entities={"asker_id": [asker_id]}`, `features=list(FEATURE_REFS)` lấy
  trực tiếp từ `lab28_platform.contracts`, `full_feature_names=False`.
- **Vì sao import `FEATURE_REFS` thay vì viết lại 4 chuỗi:** danh sách feature
  refs (`asker_activity_v1:feedback_count`, `:avg_rating`, `:negative_ratio`,
  `:delta_version`) là một contract phiên bản hóa. Sao chép nó vào hàm này tạo ra
  điểm drift: đổi feature view trong registry mà quên sửa ở đây thì Feast trả
  `NOT_FOUND` lúc serving. Một nguồn sự thật (`contracts.py`).
- **Vì sao `full_feature_names=False`:** serving path đọc theo tên feature ngắn
  (`feedback_count`) chứ không phải tên đầy đủ (`asker_activity_v1__feedback_count`);
  bật `True` sẽ làm khóa response lệch với `AskerFeatures`.
- **Trade-off:** hàm chỉ dựng payload, không validate `asker_id` — validation nằm
  ở `Identifier` trong contract HTTP trước khi tới đây.

### D. `readiness_status` — IP07 + IP08 (`/ready` semantics)

- **Chọn:** thứ tự ưu tiên rõ ràng: (1) có probe `mandatory=true` fail →
  `not_ready`; (2) không, nhưng có probe optional fail → `degraded`; (3) còn lại
  → `ready`. Đọc iterable một lần vào list rồi mới xét.
- **Vì sao tách `degraded` khỏi `not_ready`:** gateway loại pod trả 503 khỏi
  rotation. Nếu vLLM (optional khi `require_real=false`) làm cả pod bị đánh
  `not_ready`, cả API sẽ bị rút khỏi rotation dù retrieval + feature vẫn chạy —
  mất khả năng trả lời ở chế độ degraded. `degraded` = vẫn phục vụ, có nói rõ
  thiếu phần nào.
- **Vì sao `mandatory` quyết định, không phải tên dependency:** cùng một
  dependency (vLLM) là bắt buộc hay không tùy cấu hình `vllm.require_real`. Logic
  severity phải đọc cờ `mandatory` mà probe đã tính, không tự đoán theo tên.
- **Trade-off:** danh sách probe rỗng → `ready` (không có gì fail). Chấp nhận
  được vì caller (`serving_readiness`) luôn truyền đủ 5 probe.

## 3. Production gaps (những gì lab này *chưa* là production)

1. **`replication_factor=1`** trên mọi topic Kafka — một broker chết là mất dữ
   liệu. Production cần RF≥3 và `min.insync.replicas=2`.
2. **DLQ chỉ lưu, chưa có vòng xử lý tự động** — `lab28 dlq` cho replay thủ công;
   thiếu alert theo tuổi message và backoff.
3. **Feast materialization theo batch** — freshness phụ thuộc lịch Airflow; feature
   real-time (vd đếm feedback trong 5 phút) cần stream materialization.
4. **Không có schema registry** — `schema_version` là chuỗi trong payload; producer
   vẫn có thể phát bản không tương thích. Cần Confluent/Apicurio registry chặn từ
   phía broker.
5. **vLLM một replica, không autoscale** — không có hàng đợi, không có tách
   prefill/decode; tải cao sẽ timeout thay vì degrade mượt.
6. **Bí mật** — lab dùng `ports.template` không secret; production cần
   Vault/Sealed Secrets + rotation, không để config trần trong Git.
7. **Rollback mới ở tầng MLflow alias + GitOps desired-state** — chưa có
   canary/shadow traffic để bắt hồi quy chất lượng trước khi đổi champion.
8. **Quan sát**: có golden signals + trace, nhưng chưa có SLO/error budget chính
   thức và chưa gắn alert vào on-call routing.

## 4. Bằng chứng cần bổ sung khi có stack (`UNVERIFIED` cho tới lúc đó)

Chạy trên môi trường giảng viên cấp, theo README Bước 7–10:

```text
docker compose --env-file ports.template up -d --build --wait
uv run lab28 topics && uv run lab28 index --source file && uv run lab28 release
uv run lab28 seed --via-gateway && uv run lab28 inspect && uv run lab28 ready
docker compose --env-file ports.template --profile full up -d --build --wait
uv run pytest integration-tests/test_j1_golden_path.py -q
uv run pytest integration-tests/test_j2_idempotent_replay.py -q
uv run pytest integration-tests -m "not gpu and not langsmith" -q
uv run lab28 evidence && uv run lab28 integration
uv run python load-tests/run_profile.py --requests 200 --workers 8
```

Cần thu (SUBMISSION.md): `integration-report.json`, 10 file evidence đúng tên
trong `contracts/integration-matrix.yaml`, happy-path trace có run/trace/Delta/
MLflow version, failure/recovery + no-data-loss, load P50/P95/P99 + bottleneck,
K8s/GitOps drift/rollback evidence. GPU/vLLM (IP07) cần endpoint thật theo
`KAGGLE_GPU_EXTENSION.md` — server chỉ giả OpenAI API không đạt IP07.

## 5. Đóng góp từng thành viên

Làm cá nhân — toàn bộ do **Nguyễn Văn Dương** (2A202601400) thực hiện:
Part A–D, chạy fast suite / lint / verify scripts, soạn tài liệu này.

<!-- Nếu chuyển sang làm nhóm, liệt kê ở đây: tên — IP phụ trách — commit/PR. -->
