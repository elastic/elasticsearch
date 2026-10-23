/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalCredentialsExpiredException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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

    public void testCloseAbortsWhenLeftoverExceedsTrailingDrainBytes() throws IOException {
        CountingAbortable inner = new CountingAbortable(filled(TransientTypingInputStream.MAX_TRAILING_DRAIN_BYTES + 2));
        TransientTypingInputStream wrapped = new TransientTypingInputStream(inner, PATH, inner.length);
        assertEquals(1, wrapped.read());
        wrapped.close();
        assertEquals(1, inner.abortCount.get());
        assertEquals(0, inner.closeCount.get());
    }

    public void testCloseDrainsWhenLeftoverIsZero() throws IOException {
        byte[] body = new byte[] { 1, 2, 3 };
        CountingAbortable inner = new CountingAbortable(body);
        TransientTypingInputStream wrapped = new TransientTypingInputStream(inner, PATH, body.length);
        assertEquals(body.length, wrapped.read(new byte[body.length]));
        wrapped.close();
        assertEquals(0, inner.abortCount.get());
        assertEquals(1, inner.closeCount.get());
    }

    public void testCloseDrainsWhenLeftoverAtMostTrailingDrainBytes() throws IOException {
        CountingAbortable inner = new CountingAbortable(filled(TransientTypingInputStream.MAX_TRAILING_DRAIN_BYTES));
        TransientTypingInputStream wrapped = new TransientTypingInputStream(inner, PATH, inner.length);
        assertEquals(1, wrapped.read());
        wrapped.close();
        assertEquals(0, inner.abortCount.get());
        assertEquals(1, inner.closeCount.get());
    }

    public void testCloseAbortsWhenLengthUnknown() throws IOException {
        CountingAbortable inner = new CountingAbortable(filled(8));
        TransientTypingInputStream wrapped = new TransientTypingInputStream(inner, PATH);
        assertEquals(1, wrapped.read());
        wrapped.close();
        assertEquals(1, inner.abortCount.get());
        assertEquals(0, inner.closeCount.get());
    }

    public void testAbortThenCloseIsOneAction() throws IOException {
        CountingAbortable inner = new CountingAbortable(new byte[TransientTypingInputStream.MAX_TRAILING_DRAIN_BYTES + 8]);
        TransientTypingInputStream wrapped = new TransientTypingInputStream(inner, PATH, inner.length);
        wrapped.abort();
        wrapped.close();
        assertEquals(1, inner.abortCount.get());
        assertEquals(0, inner.closeCount.get());
    }

    public void testCloseThenAbortIsOneAction() throws IOException {
        CountingAbortable inner = new CountingAbortable(new byte[8]);
        TransientTypingInputStream wrapped = new TransientTypingInputStream(inner, PATH, inner.length);
        wrapped.close();
        wrapped.abort();
        assertEquals(0, inner.abortCount.get());
        assertEquals(1, inner.closeCount.get());
    }

    public void testSkipBytesAreCounted() throws IOException {
        int length = TransientTypingInputStream.MAX_TRAILING_DRAIN_BYTES + 100;
        CountingAbortable inner = new CountingAbortable(new byte[length]);
        TransientTypingInputStream wrapped = new TransientTypingInputStream(inner, PATH, length);
        assertEquals(200, wrapped.skip(200));
        wrapped.close();
        assertEquals("skip must count so leftover falls into the drain band", 0, inner.abortCount.get());
        assertEquals(1, inner.closeCount.get());
    }

    public void testCloseVersusAbortRaceAbortsAtMostOnce() throws Exception {
        // Leftover above the drain band: close() routes to abort(), so this is abort vs abort.
        assertCloseVersusAbortRaceIsOneAction(TransientTypingInputStream.MAX_TRAILING_DRAIN_BYTES + 8, true);
    }

    public void testCloseVersusAbortRaceDrainBandIsOneAction() throws Exception {
        // Leftover in the drain band: close() drains, abort() drops. First of close/abort wins.
        assertCloseVersusAbortRaceIsOneAction(8, false);
    }

    private static void assertCloseVersusAbortRaceIsOneAction(int length, boolean bothPathsAbort) throws Exception {
        for (int i = 0; i < 200; i++) {
            CountingAbortable inner = new CountingAbortable(new byte[length]);
            TransientTypingInputStream wrapped = new TransientTypingInputStream(inner, PATH, inner.length);
            Thread abortThread = new Thread(wrapped::abort);
            Thread closeThread = new Thread(() -> {
                try {
                    wrapped.close();
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
            abortThread.start();
            closeThread.start();
            abortThread.join();
            closeThread.join();
            assertEquals("exactly one of abort or close must win", 1, inner.abortCount.get() + inner.closeCount.get());
            assertThat(inner.abortCount.get(), lessThanOrEqualTo(1));
            assertThat(inner.closeCount.get(), lessThanOrEqualTo(1));
            if (bothPathsAbort) {
                assertEquals(1, inner.abortCount.get());
                assertEquals(0, inner.closeCount.get());
            }
        }
    }

    public void testReturnValueIsAbortable() {
        TransientTypingInputStream wrapped = new TransientTypingInputStream(new ByteArrayInputStream(new byte[1]), PATH, 1);
        assertThat(wrapped, instanceOf(Abortable.class));
    }

    private static byte[] filled(int length) {
        byte[] body = new byte[length];
        java.util.Arrays.fill(body, (byte) 1);
        return body;
    }

    private static final class CountingAbortable extends FilterInputStream implements Abortable {
        final int length;
        final AtomicInteger abortCount = new AtomicInteger();
        final AtomicInteger closeCount = new AtomicInteger();

        CountingAbortable(byte[] body) {
            super(new ByteArrayInputStream(body));
            this.length = body.length;
        }

        @Override
        public void abort() {
            abortCount.incrementAndGet();
        }

        @Override
        public void close() throws IOException {
            closeCount.incrementAndGet();
            super.close();
        }
    }
}
