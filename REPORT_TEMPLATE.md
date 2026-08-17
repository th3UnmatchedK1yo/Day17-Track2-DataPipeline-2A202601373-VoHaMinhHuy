# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Võ Hà Minh Huy  **MSSV:** 2A202601373  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 24.8s
  run 2/3 … 24.2s
  run 3/3 … 22.7s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  bài mở rộng (EXTRA.md)                      — chưa chạy `make seed-extra`
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau khi Clear Task và chạy lại pipeline, `gold_training_set` tăng số hàng sau mỗi lượt. Lượt đầu: 38.750 hàng (kỳ vọng 12.480). Mỗi lượt chạy thêm lại tăng thêm ~26.270 hàng. |
| **Nguyên nhân** | Model incremental `gold_training_set` không khai báo `unique_key` nên dbt sinh ra câu `INSERT INTO ... SELECT ...` thuần; mỗi lần chạy lại cùng một partition sẽ **ghi thêm** toàn bộ dữ liệu thay vì **ghi đè** hàng đã tồn tại. Thêm vào đó, source CDC có bản ghi `op='u'` (update) — một ticket tạo ngày D1 rồi sửa ngày D2 sẽ đi qua bộ lọc `WHERE _ingested_at` ở hai partition ngày khác nhau trong cùng một lượt chạy, nên ngay lượt đầu tiên đã có ticket bị lặp. Ngoài ra, DAG Airflow cấu hình `catchup=True` và `max_active_runs` không giới hạn, nghĩa là Clear Task có thể kích hoạt nhiều run đồng thời chạy bù các ngày quá khứ — nhân thêm số lần ghi trùng. |
| **Cách khắc phục** | *File `dbt/models/gold/gold_training_set.sql`:* thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`. Nhờ đó dbt sinh ra câu `MERGE` với điều kiện match trên `ticket_id` — hàng đã tồn tại sẽ được cập nhật thay vì chèn thêm. *File `dags/ai_training_pipeline.py`:* đổi `catchup=False` và `max_active_runs=1` để Airflow không tự chạy bù quá khứ và chỉ cho phép một run tại một thời điểm. |
| **Bằng chứng** | trước: 38.750 hàng (tăng mỗi lượt) · sau: 12.480 hàng · checksum 3 lượt: `8dd7c98653` (giống hệt) |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu khoảng 5% hàng so với đối chiếu thủ công (8.645 so với kỳ vọng 9.100). Các hàng bị thiếu tập trung ở các ngày cũ đã xử lý xong từ lâu, ngày mới thì đủ. |
| **P99 độ trễ đo được** | **~2.73 ngày** (P50 = 0.13 ngày, P95 = 1.81 ngày, Max = 2.94 ngày). Tỷ lệ bản ghi tới muộn hơn 1 ngày: ~5.05%. Phân bố có hai cụm rõ rệt: đa số bản ghi tới trong 0-6 giờ, một nhóm nhỏ tới muộn 43-71 giờ (~2-3 ngày). |
| **Lookback đã chọn** | 3 ngày — vì P99 ≈ 2.73 ngày, làm tròn lên 3 ngày để bao phủ gần như toàn bộ dữ liệu tới muộn. Chọn P99 thay vì max vì max (2.94 ngày) cũng nằm trong khoảng 3 ngày, và mỗi ngày lùi thêm có chi phí tính toán lặp lại ở **mọi** lượt chạy sau này (phải tính lại aggregation cho các ngày trong window), nên cần cân đối giữa độ phủ và hiệu năng. |
| **Nguyên nhân** | Điều kiện lọc incremental ban đầu là `WHERE event_date > (SELECT max(event_date) FROM target)` — chỉ xử lý event có `event_date` lớn hơn ngày mới nhất đã có trong bảng đích. Một event xảy ra ngày 08-12 nhưng tới kho ngày 08-15 sẽ **không bao giờ** được xử lý, vì tại thời điểm nạp vào Bronze, `max(event_date)` trong target đã ≥ 08-14 — điều kiện `event_date > 08-14` loại bỏ event có `event_date = 08-12`. Đây là vấn đề late-arriving data: model chỉ nhìn về phía trước, không quay lại xử lý dữ liệu tới muộn. |
| **Cách khắc phục** | *File `dbt/models/gold/gold_feature_daily.sql`:* (1) Đổi điều kiện lọc thành `WHERE event_date >= (SELECT max(event_date) FROM target) - interval 3 day` để lùi lại 3 ngày bao phủ P99. (2) Thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` để khi cùng một cặp (ngày, customer) được tính lại, kết quả mới thay thế kết quả cũ thay vì cộng dồn. |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng (đúng kỳ vọng) · checksum 3 lượt: `3db448685c` (giống hệt) |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 (2.73 ngày) đại diện cho 99% dữ liệu — là ngưỡng thực tiễn để cân bằng giữa độ phủ và chi phí. Nếu dùng max (2.94 ngày), cần lùi 3 ngày — trong trường hợp này P99 và max cùng nằm trong khoảng 3 ngày nên lookback = 3 là đủ cho cả hai. Tuy nhiên, trong thực tế max có thể là outlier cực đoan (ví dụ 30 ngày) do sự cố hạ tầng một lần — nếu dùng max làm căn cứ sẽ phải lùi quá sâu, tốn chi phí tính toán lặp lại ở mọi lượt chạy (phải re-aggregate toàn bộ dữ liệu trong window). P99 cho phép chấp nhận mất 1% bản ghi cực muộn để giữ hiệu năng tổng thể.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Team backend đổi cột `priority` từ số sang chuỗi từ 08-10 (có thông báo trên Slack). Pipeline không dừng, không báo lỗi. Nhưng `silver_tickets.priority` chứa NULL và các giá trị ngoài miền 1..4 (gồm 0, 5, -1), khiến model phân loại dự đoán kém. Tổng cộng 6.606 hàng sai trong `silver_tickets`. |
| **Nguyên nhân** | Macro `normalize_priority` ban đầu chỉ dùng `try_cast(priority_raw as integer)`, sai theo **hai hướng ngược nhau**: (1) Nó biến các nhãn chuỗi hợp lệ (`urgent`, `high`, `medium`, `low`) thành NULL — vứt bỏ dữ liệu tốt chỉ vì source đổi format (schema evolution). (2) Đồng thời nó chấp nhận các giá trị số ngoài khoảng hợp lệ (`0`, `5`, `-1`) vì chúng cast sang integer thành công — dù contract quy định chỉ 1..4. Ngoài ra, `contract: enforced: false` nghĩa là dbt không kiểm tra kiểu dữ liệu, và không có test nào ràng buộc miền giá trị. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **Nhóm 1** (`'1'`, `'2'`, `'3'`, `'4'`): Đúng contract cũ → cast sang integer, giữ nguyên. **Nhóm 2** (`'urgent'`, `'high'`, `'medium'`, `'low'`): Schema evolution — source đổi cách biểu diễn, ý nghĩa không đổi → quy về số theo mapping API: urgent=1, high=2, medium=3, low=4. **Nhóm 3** (`'P1'`, `'unknown'`, `'0'`, `'5'`, `'-1'`, `''`, `NULL`): Dữ liệu hỏng thật → trả về NULL để đưa vào quarantine. |
| **Cách khắc phục** | *(a) `dbt/macros/normalize_priority.sql`:* Viết lại macro bằng khối `CASE` xử lý đủ 3 nhóm — số hợp lệ giữ nguyên, nhãn chuỗi map về số, còn lại trả NULL. *(b) `dbt/models/silver/silver_tickets.sql`:* Thêm CTE `cleaned` lọc bỏ bản ghi có `normalize_priority IS NULL` **trước** khi xếp hạng `row_number()` — đảm bảo loại bản ghi hỏng, không loại cả ticket (ticket vẫn còn trạng thái hợp lệ từ lần update trước). *(c) `dbt/models/silver/quarantine_tickets.sql`:* Đổi `WHERE false` thành `WHERE normalize_priority('priority_raw') IS NULL` để nhặt đúng các bản ghi bị loại. *(d) `dbt/models/silver/schema.yml`:* Bật `contract: enforced: true`, thêm test `not_null` và `accepted_values: [1, 2, 3, 4]` cho cột `priority`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (đúng kỳ vọng) · `silver_tickets.priority` ∈ 1..4 sạch · `silver_tickets` đủ 12.480 ticket · `dbt test` 11/11 pass (bản gốc 9 test) |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **Chặn ở Silver, không chặn ở Bronze.** Bronze là tầng lưu trữ thô (raw), giữ nguyên dữ liệu như nguồn gửi — kể cả dữ liệu lỗi. Nếu Bronze từ chối bản ghi lỗi thì khi cần điều tra sự cố về sau (ví dụ: xác minh source gửi sai gì, từ bao giờ, bao nhiêu bản ghi), không còn bằng chứng nào để đối chiếu. Silver là tầng phù hợp để áp dụng data contract: dữ liệu đã được lưu an toàn ở Bronze, Silver chỉ chọn phần hợp lệ để phục vụ downstream.
>
> **Không để pipeline dừng** vì 312 bản ghi lỗi (trong tổng số hơn 14.000 bản ghi CDC) không có quyền chặn hơn 130.000 event và 31.200 chunk hoàn toàn bình thường đến tay người dùng. Nếu dừng cả DAG, toàn bộ dữ liệu tốt bị treo cho đến khi ai đó xử lý vài trăm bản ghi xấu — trong khi tách riêng vào `quarantine_tickets` cho phép pipeline chạy tiếp và đội vận hành xử lý bất đồng bộ.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | không làm |
| **Nguyên nhân** | — |
| **Cách khắc phục** | — |
| **Bằng chứng** | — |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra `materialized` config của mọi model incremental: có khai báo `unique_key` và `incremental_strategy` chưa? Không có `unique_key` thì mọi phép chạy lại đều là phép nhân bản — bất kỳ cơ chế retry nào ở tầng trên (Airflow Clear Task, backfill) đều biến thành nguồn sinh dữ liệu trùng. |
| 2 | Đo phân bố độ trễ giữa thời điểm sự kiện xảy ra và thời điểm dữ liệu tới kho (P50, P95, P99, max). Nếu model incremental chỉ nhìn về phía trước (`event_date > max`), bất kỳ bản ghi nào tới muộn hơn một ngày sẽ bị bỏ sót vĩnh viễn — và lỗi này ổn định qua mọi lần chạy nên không ai phát hiện. |
| 3 | Kiểm tra data contract có được bật không, test có ràng buộc miền giá trị không, và pipeline có cơ chế tách bản ghi lỗi (quarantine) thay vì dừng toàn bộ không. Schema evolution là chuyện bình thường — pipeline cần xử lý được sự thay đổi thay vì âm thầm hỏng. |
