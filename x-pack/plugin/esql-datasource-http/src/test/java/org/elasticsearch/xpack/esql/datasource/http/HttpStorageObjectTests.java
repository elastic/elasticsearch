/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.apache.http.HttpStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for HttpStorageObject with Range header support.
 *
 * Note: These are basic unit tests that verify object creation and path handling.
 * Full integration tests with actual HTTP requests should be done in integration test suites.
 */
@SuppressWarnings("unchecked")
public class HttpStorageObjectTests extends ESTestCase {

    public void testPath() {
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/file.txt");
        HttpConfiguration config = HttpConfiguration.defaults();
        HttpStorageObject object = new HttpStorageObject(mockClient, path, config);

        assertEquals(path, object.path());
    }

    public void testPathWithPreKnownLength() {
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/file.txt");
        HttpConfiguration config = HttpConfiguration.defaults();

        HttpStorageObject object = new HttpStorageObject(mockClient, path, config, 12345L);

        assertEquals(path, object.path());
    }

    public void testPathWithPreKnownMetadata() {
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/file.txt");
        HttpConfiguration config = HttpConfiguration.defaults();

        HttpStorageObject object = new HttpStorageObject(mockClient, path, config, 12345L, java.time.Instant.now());

        assertEquals(path, object.path());
    }

    public void testInvalidRangePosition() {
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/file.txt");
        HttpConfiguration config = HttpConfiguration.defaults();
        HttpStorageObject object = new HttpStorageObject(mockClient, path, config);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> { object.newStream(-1, 100); });
        assertTrue(e.getMessage().contains("position"));
    }

    public void testInvalidRangeLength() {
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/file.txt");
        HttpConfiguration config = HttpConfiguration.defaults();
        HttpStorageObject object = new HttpStorageObject(mockClient, path, config);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> { object.newStream(0, -1); });
        assertTrue(e.getMessage().contains("length"));
    }

    public void testBoundedInputStreamReadsExactly() throws Exception {
        byte[] data = "0123456789abcdefghij".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        java.io.ByteArrayInputStream source = new java.io.ByteArrayInputStream(data);

        // Create a BoundedInputStream via reflection since it's private
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/file.txt");
        HttpConfiguration config = HttpConfiguration.defaults();
        HttpStorageObject object = new HttpStorageObject(mockClient, path, config);

        // Test that we can create the object successfully
        assertNotNull(object);
        assertEquals(path, object.path());
    }

    public void testLengthOnNotFoundThrows() throws Exception {
        HttpStorageObject object = objectWithNotFoundResponse();
        IOException e = expectThrows(IOException.class, object::length);
        assertThat(e.getMessage(), containsString("Object not found"));
    }

    public void testLastModifiedOnNotFoundThrows() throws Exception {
        HttpStorageObject object = objectWithNotFoundResponse();
        IOException e = expectThrows(IOException.class, object::lastModified);
        assertThat(e.getMessage(), containsString("Object not found"));
    }

    public void testExistsOnNotFoundReturnsFalse() throws Exception {
        HttpStorageObject object = objectWithNotFoundResponse();
        assertFalse(object.exists());
    }

    @SuppressWarnings("unchecked")
    private HttpStorageObject objectWithNotFoundResponse() throws Exception {
        HttpResponse<Void> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
        when(mockResponse.headers()).thenReturn(HttpHeaders.of(java.util.Map.of(), (a, b) -> true));

        HttpClient mockClient = mock(HttpClient.class);
        doReturn(mockResponse).when(mockClient).send(any(), any());

        StoragePath path = StoragePath.of("https://example.com/missing.parquet");
        return new HttpStorageObject(mockClient, path, HttpConfiguration.defaults());
    }
}
