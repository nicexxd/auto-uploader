package com.auto.uploader

import android.app.*
import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.FileObserver
import android.os.IBinder
import androidx.core.app.NotificationCompat
import okhttp3.*
import okhttp3.RequestBody.Companion.asRequestBody
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class AutoUploadService : Service() {
    private val worker = Executors.newSingleThreadExecutor()
    private val client = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .build()

    private var obs: FileObserver? = null
    private val dedup = ConcurrentHashMap.newKeySet<String>()

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        startForegroundWithNotification()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val dir = intent?.getStringExtra("watch_dir") ?: return START_STICKY
        val uploadUrl = intent.getStringExtra("url") ?: return START_STICKY
        val token = intent.getStringExtra("token") ?: return START_STICKY
        val path = intent.getStringExtra("path")
        setupObserver(dir, uploadUrl, token, path)
        return START_STICKY
    }

    override fun onDestroy() {
        obs?.stopWatching()
        super.onDestroy()
    }

    private fun setupObserver(dir: String, url: String, token: String, path: String?) {
        obs?.stopWatching()
        val root = File(dir)
        if (!root.exists()) root.mkdirs()
        obs = object : FileObserver(dir, FileObserver.CREATE or FileObserver.MOVED_TO or FileObserver.CLOSE_WRITE) {
            override fun onEvent(event: Int, pathName: String?) {
                if (pathName == null) return
                val f = File(root, pathName)
                if (!f.isFile) return
                if (!pathName.endsWith(".wbmomo", true)) return
                val key = f.absolutePath
                if (!dedup.add(key)) return
                worker.execute { tryUpload(f, url, token, path) }
            }
        }
        obs?.startWatching()
    }

    private fun tryUpload(file: File, url: String, token: String, subPath: String?) {
        // 等待写入完成（简单策略）
        var stable = false
        var last = -1L
        repeat(10) {
            val len = file.length()
            if (len == last && len > 0) { stable = true; return@repeat }
            last = len
            Thread.sleep(500)
        }
        if (!stable) Thread.sleep(1000)

        val body = MultipartBody.Builder().setType(MultipartBody.FORM)
            .addFormDataPart("file", file.name, file.asRequestBody(null))
            .addFormDataPart("token", token)
            .apply { if (!subPath.isNullOrBlank()) addFormDataPart("path", subPath) }
            .build()
        val req = Request.Builder().url(url).post(body).build()
        try {
            client.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) {
                    // 失败后允许重试：短暂延迟后再提交一次
                    Thread.sleep(2000)
                    client.newCall(req).execute().close()
                }
            }
        } catch (_: Exception) {
            // 忽略，下一次文件事件会再尝试
        } finally {
            dedup.remove(file.absolutePath)
        }
    }

    private fun startForegroundWithNotification() {
        val channelId = "auto_uploader"
        val nm = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(channelId, "自动上传", NotificationManager.IMPORTANCE_LOW)
            nm.createNotificationChannel(channel)
        }
        val notif = NotificationCompat.Builder(this, channelId)
            .setContentTitle("自动上传正在运行")
            .setContentText("监听目录变化并自动上传 .wbmomo 文件")
            .setSmallIcon(android.R.drawable.stat_sys_upload)
            .build()
        startForeground(1, notif)
    }
}