/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalCredentialsExpiredException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Mid-body S3 faults must be typed before the resume loop sees them. Session-token error codes are
 * {@link ExternalCredentialsExpiredException}; a bare 403 or transport drop stays
 * {@link ExternalUnavailableException}.
 */
public class S3TransientTypingInputStreamTests extends ESTestCase {

    private static final StoragePath PATH = StoragePath.of("s3://bucket/key");

    public void testMidReadExpiredTokenIsCredentialsExpired() {
        S3Exception expired = s3Error(403, "ExpiredToken");
        TransientTypingInputStream wrapped = new TransientTypingInputStream(faultingStream(expired), PATH);
        ExternalCredentialsExpiredException e = expectThrows(ExternalCredentialsExpiredException.class, wrapped::read);
        assertSame(expired, e.getCause());
        assertThat(e.getMessage(), containsString("expired or invalid"));
        assertThat(e.getMessage(), containsString("HTTP 403 ExpiredToken"));
        assertThat(e.getMessage(), containsString("reading [" + PATH + "]"));
    }

    public void testMidReadExpiredTokenViaBulkReadIsCredentialsExpired() {
        TransientTypingInputStream wrapped = new TransientTypingInputStream(faultingStream(s3Error(403, "ExpiredToken")), PATH);
        ExternalCredentialsExpiredException e = expectThrows(
            ExternalCredentialsExpiredException.class,
            () -> wrapped.read(new byte[8], 0, 8)
        );
        assertThat(e.getMessage(), containsString("HTTP 403 ExpiredToken"));
    }

    public void testMidReadInvalidTokenIsCredentialsExpired() {
        TransientTypingInputStream wrapped = new TransientTypingInputStream(faultingStream(s3Error(400, "InvalidToken")), PATH);
        ExternalCredentialsExpiredException e = expectThrows(ExternalCredentialsExpiredException.class, wrapped::read);
        assertThat(e.getMessage(), containsString("HTTP 400 InvalidToken"));
    }

    public void testMidReadTokenRefreshRequiredIsCredentialsExpired() {
        TransientTypingInputStream wrapped = new TransientTypingInputStream(faultingStream(s3Error(400, "TokenRefreshRequired")), PATH);
        ExternalCredentialsExpiredException e = expectThrows(ExternalCredentialsExpiredException.class, wrapped::read);
        assertThat(e.getMessage(), containsString("HTTP 400 TokenRefreshRequired"));
    }

    public void testMidReadBareHttp403StaysUnavailable() {
        S3Exception bare = (S3Exception) S3Exception.builder().statusCode(403).message("Forbidden").build();
        TransientTypingInputStream wrapped = new TransientTypingInputStream(faultingStream(bare), PATH);
        expectThrows(ExternalUnavailableException.class, wrapped::read);
    }

    public void testMidReadClassifiesByErrorCodeNotMessage() {
        S3Exception codeWins = (S3Exception) S3Exception.builder()
            .statusCode(403)
            .message("unrelated")
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("ExpiredToken").build())
            .build();
        TransientTypingInputStream expired = new TransientTypingInputStream(faultingStream(codeWins), PATH);
        ExternalCredentialsExpiredException e = expectThrows(ExternalCredentialsExpiredException.class, expired::read);
        assertThat(e.getMessage(), containsString("HTTP 403 ExpiredToken"));

        S3Exception messageDoesNotWin = (S3Exception) S3Exception.builder()
            .statusCode(403)
            .message("ExpiredToken")
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").build())
            .build();
        TransientTypingInputStream denied = new TransientTypingInputStream(faultingStream(messageDoesNotWin), PATH);
        expectThrows(ExternalUnavailableException.class, denied::read);
    }

    public void testMidReadAccessDeniedStaysUnavailable() {
        TransientTypingInputStream wrapped = new TransientTypingInputStream(faultingStream(s3Error(403, "AccessDenied")), PATH);
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, wrapped::read);
        assertFalse(e.throttling());
    }

    public void testMidReadBareHttp400StaysUnavailable() {
        S3Exception bare = (S3Exception) S3Exception.builder().statusCode(400).message("Bad Request").build();
        TransientTypingInputStream wrapped = new TransientTypingInputStream(faultingStream(bare), PATH);
        expectThrows(ExternalUnavailableException.class, wrapped::read);
    }

    public void testMidReadAuthorizationHeaderMalformedStaysUnavailable() {
        TransientTypingInputStream wrapped = new TransientTypingInputStream(
            faultingStream(s3Error(400, "AuthorizationHeaderMalformed")),
            PATH
        );
        expectThrows(ExternalUnavailableException.class, wrapped::read);
    }

    public void testMidReadExpiredTokenNestedUnderSdkClientException() {
        S3Exception expired = s3Error(403, "ExpiredToken");
        SdkClientException nested = SdkClientException.create("Unable to execute HTTP request", expired);
        TransientTypingInputStream wrapped = new TransientTypingInputStream(faultingStream(nested), PATH);
        ExternalCredentialsExpiredException e = expectThrows(ExternalCredentialsExpiredException.class, wrapped::read);
        assertSame(nested, e.getCause());
    }

    public void testMidReadExpiredTokenWrappedInIOException() {
        S3Exception expired = s3Error(403, "ExpiredToken");
        TransientTypingInputStream wrapped = new TransientTypingInputStream(throwingStream(new IOException(expired)), PATH);
        ExternalCredentialsExpiredException e = expectThrows(ExternalCredentialsExpiredException.class, wrapped::read);
        assertThat(e.getCause(), instanceOf(IOException.class));
        assertSame(expired, e.getCause().getCause());
    }

    public void testMidReadPlainTransportIOExceptionIsTransientNotThrottling() {
        TransientTypingInputStream wrapped = new TransientTypingInputStream(throwingStream(new IOException("connection reset")), PATH);
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, wrapped::read);
        assertFalse(e.throttling());
    }

    private static InputStream faultingStream(Exception toThrow) {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                throwAsReadFailure(toThrow);
                throw new AssertionError("unreachable");
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                throwAsReadFailure(toThrow);
                throw new AssertionError("unreachable");
            }
        };
    }

    private static InputStream throwingStream(IOException toThrow) {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                throw toThrow;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                throw toThrow;
            }
        };
    }

    private static void throwAsReadFailure(Exception toThrow) throws IOException {
        if (toThrow instanceof IOException io) {
            throw io;
        }
        if (toThrow instanceof RuntimeException runtime) {
            throw runtime;
        }
        throw new AssertionError(toThrow);
    }

    private static S3Exception s3Error(int status, String errorCode) {
        return (S3Exception) S3Exception.builder()
            .statusCode(status)
            .message(errorCode)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
            .build();
    }
}
