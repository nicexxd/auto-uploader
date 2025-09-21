package com.auto.uploader

import android.app.Activity
import android.content.ContentResolver
import android.content.Intent
import android.Manifest
import android.os.Build
import android.os.Environment
import android.os.Bundle
import android.provider.OpenableColumns
import android.provider.Settings
import android.net.Uri
import android.view.View
import android.widget.*
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import okhttp3.*
import okio.BufferedSink
import java.io.InputStream
import java.util.concurrent.Executors

class MainActivity : AppCompatActivity() {
    private lateinit var urlEdit: EditText
    private lateinit var tokenEdit: EditText
    private lateinit var pathEdit: EditText
    private lateinit var pickBtn: Button
    private lateinit var uploadBtn: Button
    private lateinit var statusText: TextView
    private lateinit var dirEdit: EditText
    private lateinit var startBtn: Button
    private lateinit var stopBtn: Button

    private var pickedUri: Uri? = null
    private val http = OkHttpClient()
    private val io = Executors.newSingleThreadExecutor()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        urlEdit = findViewById(R.id.editUrl)
        tokenEdit = findViewById(R.id.editToken)
        pathEdit = findViewById(R.id.editPath)
        dirEdit = findViewById(R.id.editDir)
        pickBtn = findViewById(R.id.btnPick)
        uploadBtn = findViewById(R.id.btnUpload)
        startBtn = findViewById(R.id.btnStart)
        stopBtn = findViewById(R.id.btnStop)
        statusText = findViewById(R.id.txtStatus)

        // 默认值
        val defUrl = "http://kagfp08s13n6.ngrok.xiaomiqiu123.top/upload.php"
        val defToken = "012e909d27ee448297efaf7a52932a31"
        val defPath = "phone1/momo"
        val defDir = "/storage/emulated/0/Download/weiba/momo/"

        // 读取持久化配置
        val prefs = getSharedPreferences("cfg", MODE_PRIVATE)
        urlEdit.setText(prefs.getString("url", defUrl))
        tokenEdit.setText(prefs.getString("token", defToken))
        pathEdit.setText(prefs.getString("path", defPath))
        dirEdit.setText(prefs.getString("dir", defDir))

        // Android 13+ 通知权限（用于前台服务通知展示）
        if (Build.VERSION.SDK_INT >= 33) {
            ActivityCompat.requestPermissions(this, arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1001)
        }

        val pickLauncher = registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { res ->
            if (res.resultCode == Activity.RESULT_OK) {
                pickedUri = res.data?.data
                pickedUri?.let { uri ->
                    statusText.text = "已选择：" + getFileName(contentResolver, uri)
                    try { contentResolver.takePersistableUriPermission(uri, Intent.FLAG_GRANT_READ_URI_PERMISSION) } catch (_: Exception) {}
                }
            }
        }

        pickBtn.setOnClickListener {
            val intent = Intent(Intent.ACTION_OPEN_DOCUMENT).apply {
                addCategory(Intent.CATEGORY_OPENABLE)
                type = "*/*"
            }
            pickLauncher.launch(intent)
        }

        uploadBtn.setOnClickListener {
            val url = urlEdit.text.toString().trim()
            val token = tokenEdit.text.toString().trim()
            val path = pathEdit.text.toString().trim()
            val uri = pickedUri
            if (url.isEmpty() || token.isEmpty() || uri == null) {
                toast("请先填写URL/Token并选择文件")
                return@setOnClickListener
            }
            setLoading(true)
            io.execute {
                try {
                    val fileName = getFileName(contentResolver, uri) ?: "upload.bin"
                    val body = MultipartBody.Builder().setType(MultipartBody.FORM)
                        .addFormDataPart("file", fileName, StreamRequestBody(contentResolver, uri))
                        .addFormDataPart("token", token)
                        .apply { if (path.isNotEmpty()) addFormDataPart("path", path) }
                        .build()
                    val req = Request.Builder().url(url).post(body).build()
                    http.newCall(req).execute().use { resp ->
                        val ok = resp.isSuccessful
                        val text = resp.body?.string() ?: ""
                        runOnUiThread {
                            setLoading(false)
                            statusText.text = if (ok) "上传成功：\n$text" else "上传失败(${resp.code})：\n$text"
                        }
                    }
                } catch (e: Exception) {
                    runOnUiThread {
                        setLoading(false)
                        statusText.text = "异常：${e.message}"
                    }
                }
            }
        }

        startBtn.setOnClickListener {
            val url = urlEdit.text.toString().trim()
            val token = tokenEdit.text.toString().trim()
            val path = pathEdit.text.toString().trim()
            val dir = dirEdit.text.toString().trim()
            if (url.isEmpty() || token.isEmpty() || dir.isEmpty()) {
                toast("请填写 URL/Token/监听目录")
                return@setOnClickListener
            }
            // Android 11+ 需 MANAGE_EXTERNAL_STORAGE
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R && !Environment.isExternalStorageManager()) {
                try {
                    val intent = Intent(Settings.ACTION_MANAGE_APP_ALL_FILES_ACCESS_PERMISSION)
                    intent.data = Uri.parse("package:$packageName")
                    startActivity(intent)
                } catch (_: Exception) {
                    startActivity(Intent(Settings.ACTION_MANAGE_ALL_FILES_ACCESS_PERMISSION))
                }
                toast("请在系统设置中授予“所有文件访问”权限后再启动监听")
                return@setOnClickListener
            }
            // Android 10 及以下请求读写存储权限
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) {
                if (!ensureLegacyStoragePermissions()) return@setOnClickListener
            }
            val it = Intent(this, AutoUploadService::class.java).apply {
                putExtra("watch_dir", dir)
                putExtra("url", url)
                putExtra("token", token)
                putExtra("path", path)
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) startForegroundService(it) else startService(it)
            toast("已启动自动监听")
        }

        stopBtn.setOnClickListener {
            stopService(Intent(this, AutoUploadService::class.java))
            toast("已停止")
        }
    }

    override fun onPause() {
        super.onPause()
        // 保存配置
        val prefs = getSharedPreferences("cfg", MODE_PRIVATE)
        prefs.edit()
            .putString("url", urlEdit.text.toString())
            .putString("token", tokenEdit.text.toString())
            .putString("path", pathEdit.text.toString())
            .putString("dir", dirEdit.text.toString())
            .apply()
    }

    private fun ensureLegacyStoragePermissions(): Boolean {
        val need = mutableListOf<String>()
        if (Build.VERSION.SDK_INT <= 32) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.READ_EXTERNAL_STORAGE) != android.content.pm.PackageManager.PERMISSION_GRANTED) {
                need.add(Manifest.permission.READ_EXTERNAL_STORAGE)
            }
        }
        if (Build.VERSION.SDK_INT <= 29) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.WRITE_EXTERNAL_STORAGE) != android.content.pm.PackageManager.PERMISSION_GRANTED) {
                need.add(Manifest.permission.WRITE_EXTERNAL_STORAGE)
            }
        }
        return if (need.isNotEmpty()) {
            ActivityCompat.requestPermissions(this, need.toTypedArray(), 1002)
            false
        } else true
    }

    private fun setLoading(loading: Boolean) {
        uploadBtn.isEnabled = !loading
        pickBtn.isEnabled = !loading
        statusText.visibility = View.VISIBLE
        if (loading) statusText.text = "上传中…"
    }

    private fun toast(msg: String) = Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()

    private fun getFileName(resolver: ContentResolver, uri: Uri): String? {
        resolver.query(uri, arrayOf(OpenableColumns.DISPLAY_NAME), null, null, null)?.use { c ->
            if (c.moveToFirst()) return c.getString(0)
        }
        return null
    }
}

class StreamRequestBody(
    private val resolver: ContentResolver,
    private val uri: Uri,
    private val mime: String? = null
) : RequestBody() {
    override fun contentType(): MediaType? = mime?.let { MediaType.parse(it) }
    override fun writeTo(sink: BufferedSink) {
        resolver.openInputStream(uri)?.use { input: InputStream ->
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            while (true) {
                val read = input.read(buffer)
                if (read <= 0) break
                sink.write(buffer, 0, read)
            }
        } ?: throw IllegalStateException("无法打开所选文件")
    }
}