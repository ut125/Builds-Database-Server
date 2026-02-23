# 資料庫練習
你將從頭開始構建一個關聯資料庫伺服器。此伺服器必須接收來自標準查詢語言的請求，然後查詢並操作一組存儲的記錄。<br>
你的伺服器將以一系列檔案的形式在你的文件系統上保持持久數據。<br>
你不需要實現客戶端應用程式——此部分將由你提供的客戶端應用程式完成（讓你連接到伺服器並檢查伺服器是否正常運行）。<br>

# 如何運作
下載後整個檔案後，需要開兩個終端機來模擬 DBServer（資料庫伺服器）和 DBClient（客戶端）。

## 在第一個終端機執行 
1. 移動到專案目錄<br>
2. 使用 Maven 編譯: 'mvnw clean package'<br>
3. 運行 DBServer: 'java -cp target\classes edu.uob.DBServer'<br>
4. 等待終端機顯示: Server listening on port 8888<br>

## 在第二個終端機執行
1. 移動到專案目錄<br>
2. 運行 DBClient: 'java -cp target\classes edu.uob.DBClient'<br>
3. 在客戶端輸入 SQL 指令測試<br>

**第一個終端機**（`DBServer`）會顯示伺服器啟動和請求的處理記錄。<br>
**第二個終端機**（`DBClient`）可以輸入 SQL 查詢並獲得結果。<br>
