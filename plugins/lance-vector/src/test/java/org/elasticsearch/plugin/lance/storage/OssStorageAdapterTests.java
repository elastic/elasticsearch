/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.storage;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link OssStorageAdapter}.
 */
public class OssStorageAdapterTests extends ESTestCase {

    // ========== isOssUri tests ==========

    public void testIsOssUriWithValidOssUri() {
        assertTrue(OssStorageAdapter.isOssUri("oss://bucket-name/path/to/object"));
    }

    public void testIsOssUriWithOssUriNoPath() {
        assertTrue(OssStorageAdapter.isOssUri("oss://bucket-name/"));
    }

    public void testIsOssUriWithFileUri() {
        assertFalse(OssStorageAdapter.isOssUri("file:///path/to/file"));
    }

    public void testIsOssUriWithHttpUri() {
        assertFalse(OssStorageAdapter.isOssUri("http://example.com/path"));
    }

    public void testIsOssUriWithEmbeddedUri() {
        assertFalse(OssStorageAdapter.isOssUri("embedded:resource.json"));
    }

    public void testIsOssUriWithPlainPath() {
        assertFalse(OssStorageAdapter.isOssUri("/path/to/file"));
    }

    public void testIsOssUriWithNull() {
        assertFalse(OssStorageAdapter.isOssUri(null));
    }

    public void testIsOssUriWithEmptyString() {
        assertFalse(OssStorageAdapter.isOssUri(""));
    }

    public void testIsOssUriCaseSensitive() {
        // OSS URI should be lowercase
        assertFalse(OssStorageAdapter.isOssUri("OSS://bucket/path"));
        assertFalse(OssStorageAdapter.isOssUri("Oss://bucket/path"));
    }

    // ========== parseUri tests ==========

    public void testParseUriSimple() {
        OssStorageAdapter.OssUri uri = OssStorageAdapter.parseUri("oss://my-bucket/my-object.json");
        assertThat(uri.bucket, equalTo("my-bucket"));
        assertThat(uri.objectKey, equalTo("my-object.json"));
    }

    public void testParseUriWithNestedPath() {
        OssStorageAdapter.OssUri uri = OssStorageAdapter.parseUri("oss://bucket-name/path/to/nested/object.json");
        assertThat(uri.bucket, equalTo("bucket-name"));
        assertThat(uri.objectKey, equalTo("path/to/nested/object.json"));
    }

    public void testParseUriWithDashesAndNumbers() {
        OssStorageAdapter.OssUri uri = OssStorageAdapter.parseUri("oss://my-bucket-123/data-2024/vectors.lance");
        assertThat(uri.bucket, equalTo("my-bucket-123"));
        assertThat(uri.objectKey, equalTo("data-2024/vectors.lance"));
    }

    public void testParseUriWithEmptyPath() {
        OssStorageAdapter.OssUri uri = OssStorageAdapter.parseUri("oss://bucket/");
        assertThat(uri.bucket, equalTo("bucket"));
        assertThat(uri.objectKey, equalTo(""));
    }

    public void testParseUriInvalidFormatMissingSlash() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> OssStorageAdapter.parseUri("oss://bucket-no-path")
        );
        assertThat(ex.getMessage(), containsString("Invalid OSS URI format"));
    }

    public void testParseUriInvalidFormatNotOss() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> OssStorageAdapter.parseUri("file:///path/to/file")
        );
        assertThat(ex.getMessage(), containsString("Invalid OSS URI format"));
    }

    public void testParseUriInvalidFormatEmptyBucket() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> OssStorageAdapter.parseUri("oss:///path/to/file"));
        assertThat(ex.getMessage(), containsString("Invalid OSS URI format"));
    }

    // ========== Credentials loading tests ==========

    public void testCredentialsFileNotFound() throws Exception {
        // Create a temp directory and use a non-existent file path within it
        // This avoids entitlement issues with checking arbitrary filesystem paths
        Path tempDir = createTempDir();
        Path nonExistentFile = tempDir.resolve("nonexistent_credentials.json");

        IOException ex = expectThrows(IOException.class, () -> new OssStorageAdapter(nonExistentFile.toString()));
        assertThat(ex.getMessage(), containsString("OSS credentials file not found"));
    }

    public void testCredentialsMissingAccessKeyId() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            {
              "access_key_secret": "secret",
              "endpoint": "oss-cn-hangzhou.aliyuncs.com",
              "region": "cn-hangzhou"
            }
            """);

        IOException ex = expectThrows(IOException.class, () -> new OssStorageAdapter(tempFile.toString()));
        assertThat(ex.getMessage(), containsString("access_key_id"));
    }

    public void testCredentialsMissingAccessKeySecret() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            {
              "access_key_id": "key",
              "endpoint": "oss-cn-hangzhou.aliyuncs.com",
              "region": "cn-hangzhou"
            }
            """);

        IOException ex = expectThrows(IOException.class, () -> new OssStorageAdapter(tempFile.toString()));
        assertThat(ex.getMessage(), containsString("access_key_secret"));
    }

    public void testCredentialsMissingEndpoint() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            {
              "access_key_id": "key",
              "access_key_secret": "secret",
              "region": "cn-hangzhou"
            }
            """);

        IOException ex = expectThrows(IOException.class, () -> new OssStorageAdapter(tempFile.toString()));
        assertThat(ex.getMessage(), containsString("endpoint"));
    }

    public void testCredentialsMissingRegion() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            {
              "access_key_id": "key",
              "access_key_secret": "secret",
              "endpoint": "oss-cn-hangzhou.aliyuncs.com"
            }
            """);

        IOException ex = expectThrows(IOException.class, () -> new OssStorageAdapter(tempFile.toString()));
        assertThat(ex.getMessage(), containsString("region"));
    }

    public void testCredentialsInvalidJson() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, "not valid json");

        expectThrows(Exception.class, () -> new OssStorageAdapter(tempFile.toString()));
    }

    public void testCredentialsNonStringField() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            {
              "access_key_id": 12345,
              "access_key_secret": "secret",
              "endpoint": "oss-cn-hangzhou.aliyuncs.com",
              "region": "cn-hangzhou"
            }
            """);

        IOException ex = expectThrows(IOException.class, () -> new OssStorageAdapter(tempFile.toString()));
        assertThat(ex.getMessage(), containsString("access_key_id"));
    }

    public void testCredentialsNullField() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            {
              "access_key_id": null,
              "access_key_secret": "secret",
              "endpoint": "oss-cn-hangzhou.aliyuncs.com",
              "region": "cn-hangzhou"
            }
            """);

        IOException ex = expectThrows(IOException.class, () -> new OssStorageAdapter(tempFile.toString()));
        assertThat(ex.getMessage(), containsString("access_key_id"));
    }

    public void testValidCredentialsLoads() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            {
              "access_key_id": "test-key-id",
              "access_key_secret": "test-secret",
              "endpoint": "oss-cn-hangzhou.aliyuncs.com",
              "region": "cn-hangzhou"
            }
            """);

        // Should not throw
        OssStorageAdapter adapter = new OssStorageAdapter(tempFile.toString());
        assertNotNull(adapter);
    }

    // ========== OssCredentials tests ==========

    public void testOssCredentialsRecord() {
        OssStorageAdapter.OssCredentials creds = new OssStorageAdapter.OssCredentials("key-id", "secret", "endpoint.com", "region-1");

        assertThat(creds.accessKeyId, equalTo("key-id"));
        assertThat(creds.accessKeySecret, equalTo("secret"));
        assertThat(creds.endpoint, equalTo("endpoint.com"));
        assertThat(creds.region, equalTo("region-1"));
    }

    // ========== OssUri tests ==========

    public void testOssUriRecord() {
        OssStorageAdapter.OssUri uri = new OssStorageAdapter.OssUri("bucket-name", "object/key");

        assertThat(uri.bucket, equalTo("bucket-name"));
        assertThat(uri.objectKey, equalTo("object/key"));
    }

    // ========== listObjects tests ==========

    public void testListObjectsReturnsExpectedFiles() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            {
              "access_key_id": "key",
              "access_key_secret": "secret",
              "endpoint": "oss-cn-hangzhou.aliyuncs.com",
              "region": "cn-hangzhou"
            }
            """);

        OssStorageAdapter adapter = new OssStorageAdapter(tempFile.toString());
        String[] files = adapter.listObjects("oss://bucket/path", "prefix");

        // Phase 1 implementation returns hardcoded list
        assertThat(files.length, equalTo(3));
        assertThat(files[0], equalTo("_latest.manifest"));
        assertThat(files[1], equalTo("data.lance"));
        assertThat(files[2], equalTo("_versions/1.manifest"));
    }
}
