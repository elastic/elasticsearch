package org.elasticsearch.cloud.qiniu;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.Auth;
import com.qiniu.util.StringMap;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;

/**
 * Created by $Jason.Zhang on 10/25/17.
 */
public class QiniuKodoClient {
    private static final String KODO_DELETE_AFTER_DAYS = "deleteAfterDays";
    private Auth auth;
    private String bucketDomain;
    private BucketManager bucketManager;
    private String endPoint;
    private UploadManager uploadManager;
    private Client httpClient;

    public QiniuKodoClient(String ak, String sk, String bucketDomain, String endPoint) {
        this.auth = Auth.create(ak, sk);
        this.bucketDomain = bucketDomain;
        this.bucketManager = new BucketManager(this.auth);
        this.endPoint = endPoint;
        this.uploadManager = new UploadManager();
    }

    public QiniuKodoClient(Settings settings, Auth auth) {
        this.auth = auth;
    }

    private String getDownloadUrl(String key) throws IOException {
        String encodedFileName = URLEncoder.encode(key, "utf-8");
        String url = this.bucketDomain + "/" + encodedFileName;
        url = this.auth.privateDownloadUrl(url, 3600L);
        return url;
    }

    public String generateUploadTokenWithRetention(String bucket, String key, int retention) throws QiniuException {
        StringMap properties = new StringMap();
        properties.put("scope", bucket + ":" + key);
        if (retention > 0) {
            properties.put(KODO_DELETE_AFTER_DAYS, retention);
        }

        return this.auth.uploadToken(bucket, key, 3600L, properties);
    }

    public FileInfo stat(String bucket, String key) throws QiniuException {
        return this.bucketManager.stat(bucket, key);
    }

    public void delete(String bucket, String key) throws QiniuException {
        this.bucketManager.delete(bucket, key);
    }

    public InputStream getObject(String key) throws IOException {
        URL fileUrl = new URL(this.getDownloadUrl(key));
        return fileUrl.openConnection().getInputStream();
    }

    public boolean isBucketExist(String name) throws QiniuException {
        String[] bucket = this.bucketManager.buckets();
        for (String b : bucket) {
            if (name.equals(b)) {
                return true;
            }
        }
        return false;
    }

    public FileListing listObjects(String bucket, String prefix, String marker, int limit) throws QiniuException {
        return this.bucketManager.listFiles(bucket, prefix, marker, limit, "");
    }

    public void move(String bucket, String sourceKey, String targetKey) throws QiniuException {
        this.bucketManager.move(bucket, sourceKey, bucket, targetKey, true);
    }

    public void deleteObjects(String bucket, String[] keys) throws QiniuException {
        BucketManager.Batch deleteOp = (new BucketManager.Batch()).delete(bucket, keys);
        this.bucketManager.batch(deleteOp);
    }

    public Response doUpload(byte[] data) {
        return null;
    }


    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public Client getHttpClient() {
        return httpClient;
    }

    public void setHttpClient(Client httpClient) {
        this.httpClient = httpClient;
    }
}
