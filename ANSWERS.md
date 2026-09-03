# ANSWERS — Day 28 Track 2

> Làm cá nhân — Nguyễn Văn Dương (2A202601400). Nhánh `ca-nhan-duong`, PR #1.
> Máy làm bài: Windows 11, 8 CPU, ~8 GB đĩa trống → `preflight` = `browser-fallback`.
> Bước 1–6 chạy tại máy; Bước 7–10 chạy trên **GitHub Codespaces** (4-core/16 GB,
> docker-in-docker) vì máy cá nhân không đủ đĩa cho Docker. IP07 (vLLM thật) không
> có GPU → `UNVERIFIED`.

## 1. Trạng thái checkpoint

### Phần code (Bước 1–6) — verify tại máy + CI xanh 3 HĐH

| Checkpoint | Lệnh | Kết quả |
|---|---|---|
| Fast suite | `uv run pytest starter-tests tests -q` | 87 passed |
| Starter — Part A | `pytest -k event_headers` | 1 passed, 3 deselected |
| Starter — Part B | `pytest tests/test_delta_merge_idempotency.py` | 22 passed |
| Lint | `uv run ruff check .` | All checks passed |
| Matrix | `scripts/verify_matrix.py` | 245 checks passed |
| Portability | `scripts/check_portability.py` | OK |
| Manifests | `scripts/validate_manifests.py` | passed |
| Compose config | `docker compose … config --quiet` | exit 0 |
| CI (PR #1) | `student-ci` trên ubuntu + macos + windows | ✅ passed |

Chỉ sửa một file cho bài tập: `src/lab28_platform/integration_tasks.py`. Không sửa test.

### Luồng live (Bước 7–10) — chạy trên Codespaces

Stack `--profile full` lên đủ **14 service `healthy`** (kafka, spark-connect, airflow,
feast, qdrant, mlflow, api, gateway, jaeger, otel-collector, prometheus, grafana,
pushgateway, kafka-exporter).

| IP | Boundary | Bằng chứng quan sát được | Trạng thái |
|---|---|---|---|
| IP01 | HTTP → Kafka | `lab28 seed` → event trên `data.raw` kèm header `traceparent` + `idempotency-key`, mỗi event có `trace_id` | ✅ |
| IP02 | Kafka → Airflow | DAG `lab28_ingestion_pipeline` chạy, task `drain_kafka_into_delta` consume, phát asset event `lab28://delta/feedback` | ✅ |
| IP03 | Airflow/Spark → Delta | MERGE tạo `feedback` v1 (13 rows) + `documents` v1 (14 rows); `last_operation: MERGE`; 48 message thô (có replay) gộp còn 27 row — một dòng / `idempotency_key` (hàm `dedupe_latest`) | ✅ |
| IP04 | Delta → Feast | J1 `test_the_feature_store_serves_the_new_asker` pass — Feast serve feature cho asker vừa ghi qua pipeline | ✅ |
| IP05 | Delta docs → Qdrant | `lab28 index --source file` → 13 point, collection `lab28_documents`, ID suy ra từ `doc_id` (deterministic); `UT-vector-stable-ids` pass. Task auto-index trong DAG bị chặn bởi HF 429 (xem §5) | ✅ (CLI) / ⚠️ (pipeline) |
| IP06 | Eval → MLflow Registry | `lab28 release` → `lab28-rag-release` v1, alias `champion`, có signature + tags + provenance | ✅ |
| IP07 | RAG prompt → vLLM thật | Không có endpoint GPU/vLLM thật | ❌ `UNVERIFIED` |
| IP08 | Client → Envoy gateway | `seed --via-gateway` → 200 + **429 `local_rate_limited`** (token bucket 10 req/s trong `gateway/envoy.yaml`); request có `x-request-id` | ✅ |
| IP09 | Components → Prometheus/Grafana | Prometheus targets up; Grafana dashboard provisioned; `lab28 ready` publish per-component gauge | ✅ |
| IP10 | Components → OTLP trace | J1 `test_the_journey_is_queryable_by_its_trace_id` pass — một trace ID mang các span bắt buộc từ `lab28.gateway.request` → `lab28.spark.delta_merge` | ✅ |

**J1 golden path:** lần chạy tốt nhất **10 passed / 2 failed / 3 skipped** (3 skip là gate
`gpu`). Hai fail đều do task embedding của pipeline tải model fastembed từ HuggingFace
bị **HTTP 429** trên IP dùng chung của Codespaces (§5) — giới hạn hạ tầng, không phải
lỗi platform. Các nhánh Delta / Feast / trace của J1 đều pass.

**J2–J5, `lab28 evidence`, `lab28 integration`, load profile:** `UNVERIFIED` — hết thời
gian buổi lab với môi trường Codespaces.

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

## 4. Thay đổi hạ tầng để chạy được trên Codespaces

Ngoài `integration_tasks.py`, đã sửa các file hạ tầng (không phải test, không phải
lời giải bài tập) để vượt lỗi môi trường:

| File | Vì sao | Sửa gì |
|---|---|---|
| `.devcontainer/Dockerfile` + `devcontainer.json` | Build Codespace fail: image nền có apt source Yarn với khóa GPG đã xoay → `apt-get update` fail → feature `docker-in-docker` không cài được → Codespace rơi vào recovery mode | Xoá apt source Yarn trước khi feature chạy; nâng `hostRequirements` lên 4 CPU/16 GB/32 GB cho `--profile full`; forward thêm cổng gateway/Grafana/Jaeger/MLflow/Qdrant/Airflow; `postCreate` thêm `--extra integration` |
| `compose.yaml` | Task `index_new_documents` tải model fastembed từ HuggingFace lúc chạy; IP dùng chung của Codespaces bị HF **429** → task fail → DAG fail | Truyền `HF_TOKEN` từ shell vào các container (không ghi ra file, không commit) |
| `docs/codespaces-runbook.md` (mới) | — | Runbook chạy Bước 7–10 trên Codespaces |

## 5. Vấn đề môi trường Codespaces gặp phải (để giảng viên tham khảo)

1. **Consumer poll race:** `BatchConsumer.poll_batch` chỉ chờ 3 lần poll × 1 s = 3 s.
   Trên CPU chia sẻ, consumer Kafka mới subscribe cần lâu hơn thế để nhận partition
   assignment → run đầu tiên của DAG poll 0 message, Delta không được tạo.
   **Workaround:** trigger DAG một lần để tạo consumer group, các lần sau rejoin nhanh
   → MERGE chạy. Đã xác nhận: sau warm-up, Delta có `feedback` v1 (13 rows) +
   `documents` v1 (14 rows).
2. **Kafka không có volume** trong `compose.yaml` → mỗi lần Codespace stop/restart
   mất toàn bộ topic + message, phải `lab28 topics` + `lab28 seed` lại.
3. **HuggingFace 429** cho download model fastembed (dense + sparse) từ IP Codespaces,
   cả anonymous lẫn có token. Cách vượt: `HF_ENDPOINT=https://hf-mirror.com`, hoặc
   pre-seed `.lab28/fastembed` từ một máy IP khác. `lab28 index --source file` chạy
   trên host Codespace **có** lúc tải được (13 point trong Qdrant) trước khi IP bị
   rate-limit nặng.

## 6. Bằng chứng còn thiếu để hoàn thiện (`UNVERIFIED`)

Chạy tiếp trên Codespaces (đã có runbook `docs/codespaces-runbook.md`) hoặc môi
trường giảng viên cấp:

```text
uv run pytest integration-tests -m "not gpu and not langsmith" -q   # J1–J5
uv run lab28 evidence && uv run lab28 integration
uv run python load-tests/run_profile.py --requests 200 --workers 8
```

Cần thu (SUBMISSION.md): `integration-report.json`, 10 file `evidence/ipXX-*.json`,
happy-path trace có run/trace/Delta/MLflow version, failure/recovery + no-data-loss,
load P50/P95/P99 + bottleneck, K8s/GitOps drift/rollback. IP07 cần endpoint vLLM
thật theo `KAGGLE_GPU_EXTENSION.md`.

## 7. Đóng góp từng thành viên

Làm cá nhân — toàn bộ do **Nguyễn Văn Dương** (2A202601400) thực hiện:
Part A–D, chạy fast suite / lint / verify scripts, dựng và vận hành stack đầy đủ
trên Codespaces (Bước 7–8), chẩn đoán và vá các lỗi hạ tầng ở §4–§5, soạn tài liệu.
