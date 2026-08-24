/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3StorageObjectReadFailureTests extends ESTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/file.parquet";
    private static final StoragePath PATH = StoragePath.of("s3://" + BUCKET + "/" + KEY);

    public void testBareIllegalStateExceptionIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        IllegalStateException ise = new IllegalStateException("Connection pool shut down");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(ise);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertSame(ise, eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
        assertFalse(eue.throttling());
    }

    public void testSdkClientExceptionWrappingIllegalStateExceptionIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        IllegalStateException ise = new IllegalStateException("Connection pool shut down");
        SdkClientException wrapped = SdkClientException.create("Unable to execute HTTP request", ise);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(wrapped);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertSame(wrapped, eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
        assertNotNull(ExceptionsHelper.unwrap(eue, IllegalStateException.class));
    }

    public void testNoSuchKeyStaysIoException() {
        S3Client mockS3 = mock(S3Client.class);
        NoSuchKeyException missing = NoSuchKeyException.builder().statusCode(404).message("Not Found").build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(missing);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        IOException io = expectThrows(IOException.class, obj::newStream);
        assertEquals("Object not found: " + PATH, io.getMessage());
        assertSame(missing, io.getCause());
    }

    public void testProgrammingIllegalStateExceptionStays500() {
        S3Client mockS3 = mock(S3Client.class);
        IllegalStateException ise = new IllegalStateException("broken invariant");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(ise);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        IllegalStateException thrown = expectThrows(IllegalStateException.class, obj::newStream);
        assertSame(ise, thrown);
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(thrown));
    }

    public void testClosedClientOnLengthIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        IllegalStateException ise = new IllegalStateException("Connection pool shut down");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(ise);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::length);
        assertSame(ise, eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
        assertFalse(eue.throttling());
    }

    public void testClosedClientOnHeadFallbackIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        S3Exception serverError = (S3Exception) S3Exception.builder().statusCode(500).message("Internal Error").build();
        IllegalStateException ise = new IllegalStateException("Connection pool shut down");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(serverError);
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenThrow(ise);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::length);
        assertSame(ise, eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
    }
}
