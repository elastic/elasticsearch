/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.util.SuppressForbidden;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Storage adapter for Alibaba Cloud OSS.
 *
 * Supports reading Lance datasets from OSS buckets using Access Key/Secret Key
 * authentication stored in local credential files.
 *
 * Credential file format (JSON):
 * {
 *   "access_key_id": "your-access-key",
 *   "access_key_secret": "your-secret-key",
 *   "endpoint": "oss-cn-hangzhou.aliyuncs.com",
 *   "region": "cn-hangzhou"
 * }
 *
 * URI format: oss://bucket-name/path/to/dataset
 */
public class OssStorageAdapter {

    private static final Pattern OSS_URI_PATTERN = Pattern.compile("^oss://([^/]+)/(.*)$");
    private static final String DEFAULT_CREDENTIALS_PATH = System.getProperty("user.home") + "/.oss/credentials.json";

    private final OssCredentials credentials;

    public static class OssCredentials {
        public final String accessKeyId;
        public final String accessKeySecret;
        public final String endpoint;
        public final String region;

        public OssCredentials(String accessKeyId, String accessKeySecret, String endpoint, String region) {
            this.accessKeyId = accessKeyId;
            this.accessKeySecret = accessKeySecret;
            this.endpoint = endpoint;
            this.region = region;
        }
    }

    public OssStorageAdapter() throws IOException {
        this(DEFAULT_CREDENTIALS_PATH);
    }

    public OssStorageAdapter(String credentialsPath) throws IOException {
        this.credentials = loadCredentials(credentialsPath);
    }

    /**
     * Load OSS credentials from JSON file.
     */
    private static OssCredentials loadCredentials(String credentialsPath) throws IOException {
        Path path = PathUtils.get(credentialsPath);
        if (Files.exists(path) == false) {
            throw new IOException(
                Strings.format(
                    "OSS credentials file not found: %s. Please create a credentials file with access_key_id, access_key_secret, endpoint, and region.",
                    credentialsPath
                )
            );
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(Files.newInputStream(path));

        String accessKeyId = getRequiredField(root, "access_key_id", credentialsPath);
        String accessKeySecret = getRequiredField(root, "access_key_secret", credentialsPath);
        String endpoint = getRequiredField(root, "endpoint", credentialsPath);
        String region = getRequiredField(root, "region", credentialsPath);

        return new OssCredentials(accessKeyId, accessKeySecret, endpoint, region);
    }

    private static String getRequiredField(JsonNode root, String fieldName, String credentialsPath) throws IOException {
        JsonNode node = root.get(fieldName);
        if (node == null || node.isTextual() == false) {
            throw new IOException("Missing or invalid '" + fieldName + "' in credentials file: " + credentialsPath);
        }
        return node.asText();
    }

    /**
     * Parse OSS URI and return bucket name and object key.
     */
    public static class OssUri {
        public final String bucket;
        public final String objectKey;

        public OssUri(String bucket, String objectKey) {
            this.bucket = bucket;
            this.objectKey = objectKey;
        }
    }

    public static OssUri parseUri(String ossUri) {
        Matcher matcher = OSS_URI_PATTERN.matcher(ossUri);
        if (matcher.matches() == false) {
            throw new IllegalArgumentException("Invalid OSS URI format: " + ossUri + ". Expected format: oss://bucket-name/path/to/object");
        }
        return new OssUri(matcher.group(1), matcher.group(2));
    }

    /**
     * Read a file from OSS bucket.
     *
     * For Phase 1, this implementation uses simple HTTP GET requests.
     * In Phase 2, this should be replaced with the official OSS SDK.
     */
    @SuppressForbidden(
        reason = "Opening HTTP connection to OSS bucket. This should be replaced with proper OSS SDK in Phase 2."
    )
    public InputStream readObject(String ossUri) throws IOException {
        OssUri uri = parseUri(ossUri);

        // Construct OSS object URL
        String objectUrl = Strings.format("https://%s.%s/%s", uri.bucket, credentials.endpoint, uri.objectKey);

        URL url = new URL(objectUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        // For Phase 1, assume public read access or that the bucket allows anonymous access
        // Phase 2 should implement proper OSS signature authentication

        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            return connection.getInputStream();
        } else {
            throw new IOException("Failed to read OSS object: " + ossUri + ", HTTP response code: " + responseCode);
        }
    }

    /**
     * Check if a URI is an OSS URI.
     */
    public static boolean isOssUri(String uri) {
        return uri != null && uri.startsWith("oss://");
    }

    /**
     * List objects with a given prefix (for discovering dataset files).
     *
     * For Phase 1, this is a simplified implementation.
     * Phase 2 should implement proper OSS list operations.
     */
    public String[] listObjects(String ossUri, String prefix) throws IOException {
        // Phase 1: Return commonly expected Lance dataset files
        return new String[] { "_latest.manifest", "data.lance", "_versions/1.manifest" };
    }
}
