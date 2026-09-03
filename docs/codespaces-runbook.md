# Chạy stack trên GitHub Codespaces (Hướng B)

Máy làm bài không đủ đĩa cho Docker (`preflight` = `browser-fallback`). Codespaces
chạy `.devcontainer/` trên cloud với docker-in-docker, không tốn đĩa máy cá nhân.
Bước 1–6 đã hoàn thành và verify ở local; runbook này là Bước 7–10.

## 0. Tạo Codespace (thao tác tay, trên trình duyệt)

1. Vào `https://github.com/Duongw171/Day28-2A202601400-NguyenVanDuong`.
2. Nếu tab **Actions** hiện banner *"Workflows aren't being run on this forked
   repository"* → bấm **"I understand my workflows, go ahead and enable them"**
   (repo là fork của `VinUni-AI20k/...` nên Actions bị tắt mặc định). Bước này chỉ
   để CI của PR chạy; không bắt buộc cho Codespaces.
3. Nút **Code → Codespaces → tab ⋯ → New with options…**
   - Branch: `ca-nhan-duong`
   - Dev container configuration: `Lab 28 browser fallback`
   - Machine type: **4-core / 16 GB / 32 GB** (2-core không đủ cho profile `full`)
4. Chờ build xong (`postCreateCommand` tự chạy `uv sync --python 3.11 --extra dev`).

Tất cả lệnh dưới đây chạy trong **terminal của Codespace**.

## 1. Chuẩn bị

```bash
uv sync --frozen --python 3.11 --extra dev --extra integration --no-editable
uv run lab28 preflight        # trong Codespace phải ra local-standard/full, không còn browser-fallback
docker info                    # xác nhận docker-in-docker sống
```

## 2. Stack cơ bản — Bước 7 (IP01, IP05, IP06, IP08, IP09, IP10 một phần)

```bash
docker compose --env-file ports.template up -d --build --wait
docker compose --env-file ports.template ps
uv run lab28 topics
uv run lab28 index --source file        # kỳ vọng points_upserted > 0
uv run lab28 release                     # kỳ vọng có MLflow version + alias champion
uv run lab28 seed --via-gateway          # documents/feedback accepted, không rejected
uv run lab28 inspect
uv run lab28 ready                        # ready hoặc degraded (LLM degraded là bình thường khi chưa nối vLLM thật)
```

UI: mở tab **Ports** của Codespaces, click biểu tượng globe để forward:
`8080` gateway · `8000` API docs · `3000` Grafana · `9090` Prometheus ·
`16686` Jaeger · `5000` MLflow · `6333` Qdrant.

## 3. Stack full + 5 journey — Bước 8 (IP02, IP03, IP04, IP06, IP07-degraded)

```bash
docker compose --env-file ports.template --profile full up -d --build --wait
uv run lab28 seed --via-gateway
uv run pytest integration-tests/test_j1_golden_path.py -q
uv run pytest integration-tests/test_j2_idempotent_replay.py -q
uv run pytest integration-tests -m "not gpu and not langsmith" -q
```

Airflow UI (`:8082`, user `airflow` / pass `admin`): DAG `lab28_ingestion_pipeline`,
đối chiếu log từng task với Delta / Feast / Qdrant / MLflow.

Kỳ vọng: J1 luồng đủ API→Kafka→Airflow→Delta→Feast/Qdrant→trả lời; J2 replay không
tăng số dòng; J3 promotion+rollback alias; J4 optional dependency fail → `degraded`
→ khôi phục; J5 trace + metric xuyên suốt.

## 4. Bằng chứng + load — Bước 10

```bash
uv run lab28 evidence            # ghi evidence/*.json (thư mục này .gitignore)
uv run lab28 integration         # score 10 IP theo matrix, xuất integration-report.json
uv run python load-tests/run_profile.py --requests 200 --workers 8   # P50/P95/P99
uv run python scripts/validate_manifests.py
```

Nộp (SUBMISSION.md): `integration-report.json`, 10 file `evidence/ipXX-*.json` đúng
tên trong `contracts/integration-matrix.yaml`, happy-path trace (run/trace/Delta/
MLflow version), failure/recovery + no-data-loss, load P50/P95/P99 + bottleneck,
K8s/GitOps drift/rollback. `evidence/` bị gitignore → đính kèm vào PR hoặc nộp
LMS, hoặc `git add -f evidence/ integration-report.json` nếu lớp muốn trong repo.

## 5. IP07 — vLLM thật (GPU, ngoài Codespaces)

Codespaces không có GPU. Theo `KAGGLE_GPU_EXTENSION.md`:

```bash
# Trong Kaggle notebook (T4)
!pip install -q "vllm==0.26.0"
!vllm serve Qwen/Qwen3-4B-Instruct-2507 --host 0.0.0.0 --port 8000 \
  --dtype half --max-model-len 4096 --gpu-memory-utilization 0.85
```

Expose endpoint (tunnel), rồi trong Codespace:

```bash
export LAB28_VLLM_BASE_URL="https://<tunnel>/v1"
export LAB28_VLLM_MODEL_ID="Qwen/Qwen3-4B-Instruct-2507"
export LAB28_VLLM_REQUIRE_REAL=true
docker compose --env-file ports.template up -d --build api gateway
uv run pytest integration-tests -m gpu -q
uv run lab28 ask "Chính sách hoàn tiền là gì?"     # evidence phải có vllm_model_id + trace_id
```

Gate kiểm `/version` (bản vLLM thật), `/v1/models` (model id đã cấu hình), metric
`vllm:`. Server chỉ giả OpenAI API → **không đạt IP07**. Không hard-code URL tunnel
/ token vào repo. Nếu lớp không cấp GPU → báo `UNVERIFIED` trong `ANSWERS.md`.

## 6. Dọn

```bash
docker compose --env-file ports.template --profile full down --remove-orphans
```

Không dùng `uv run lab28 reset --yes` giữa buổi demo (xóa state trước sự cố).
Tắt Codespace khi không dùng để không tốn quota (60 giờ/tháng free).
