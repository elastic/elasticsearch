/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Direct coverage of the open-ended ({@code READ_TO_END}) S3 read: it issues a suffix range header
 * {@code bytes=position-} (no up-front {@code length()}/HEAD) and answers a past-the-end read (HTTP 416) with
 * an empty stream, per the SPI contract.
 */
public class S3OpenEndedReadTests extends ESTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/file.ndjson";
    private static final StoragePath PATH = StoragePath.of("s3://" + BUCKET + "/" + KEY);

    private final S3Client mockS3 = mock(S3Client.class);

    public void testReadToEndBuildsSuffixRangeHeaderAndDeliversBody() throws IOException {
        byte[] body = "hello open-ended world".getBytes(StandardCharsets.UTF_8);
        GetObjectResponse resp = GetObjectResponse.builder()
            .contentRange("bytes 0-" + (body.length - 1) + "/" + body.length)
            .contentLength((long) body.length)
            .build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(
            new ResponseInputStream<>(resp, AbortableInputStream.create(new ByteArrayInputStream(body)))
        );
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);

        byte[] got;
        try (InputStream in = obj.newStream(0, StorageObject.READ_TO_END)) {
            got = in.readAllBytes();
        }
        assertArrayEquals(body, got);

        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockS3).getObject(captor.capture());
        assertEquals("open-ended read must use an end-less suffix range header", "bytes=0-", captor.getValue().range());
    }

    public void testReadToEndPastEndReturnsEmptyStream() throws IOException {
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(
            (S3Exception) S3Exception.builder().statusCode(416).message("Range Not Satisfiable").build()
        );
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);

        try (InputStream in = obj.newStream(100, StorageObject.READ_TO_END)) {
            assertEquals("open-ended read at/after the end is an empty stream", -1, in.read());
        }
    }
}
