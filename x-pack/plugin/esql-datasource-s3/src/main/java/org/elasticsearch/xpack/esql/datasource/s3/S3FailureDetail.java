/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalCredentialsExpiredException;

import java.util.Set;

/**
 * Renders why an S3 call failed, for appending to the message of the {@link java.io.IOException} that carries it up
 * to ES|QL.
 * <p>
 * It exists because naming only the operation and the path is not enough to act on: a refused connection, a wrong
 * access key, and a bucket that does not exist all produce the same "&lt;operation&gt; failed for &lt;path&gt;", and
 * the part that says which one it was survives only in a nested cause that most clients never render. Every site in
 * this plugin that turns an SDK failure into an {@code IOException} appends this, so the four operations
 * (HeadObject, range GET, existence probe, object read) report the condition the same way.
 * <p>
 * For an {@link S3Exception} that means the HTTP status plus the store's own error code ({@code AccessDenied},
 * {@code NoSuchBucket}) rather than the SDK's full {@code toString}, which also carries a request id, an attempt
 * count and the configured credentials provider's identity — noise here, and in the provider's case an object hash
 * that differs between runs.
 */
final class S3FailureDetail {

    // Real SDK chains here are 2-4 deep; this only stops a pathological one.
    private static final int MAX_CAUSE_DEPTH = 12;

    /**
     * AWS session-token failures. Matched by error code only — a bare HTTP 400/403 is not expiry
     * (Hadoop's HEAD trap, malformed ranges, {@code AuthorizationHeaderMalformed}).
     * {@code TokenRefreshRequired} is in this set because Standard already refuses the 400;
     * minting a new {@code managed_identity}/{@code federated_identity} signature is a separate
     * credential-refresh step, not a retry of the same signed GET.
     */
    private static final Set<String> CREDENTIALS_EXPIRED_CODES = Set.of("ExpiredToken", "InvalidToken", "TokenRefreshRequired");

    private S3FailureDetail() {}

    @Nullable
    static S3Exception findCredentialsExpired(Throwable cause) {
        Throwable current = cause;
        for (int depth = 0; depth < MAX_CAUSE_DEPTH && current != null; depth++) {
            if (current instanceof S3Exception s3 && isCredentialsExpiredCode(s3)) {
                return s3;
            }
            Throwable next = current.getCause();
            if (next == null || next == current) {
                break;
            }
            current = next;
        }
        return null;
    }

    static boolean isCredentialsExpiredCode(S3Exception s3) {
        if (s3.awsErrorDetails() == null) {
            return false;
        }
        String code = s3.awsErrorDetails().errorCode();
        return code != null && CREDENTIALS_EXPIRED_CODES.contains(code);
    }

    // action is the gerund after "Session credentials expired or invalid"
    // ("reading [s3://…]", "listing objects…").
    @Nullable
    static ExternalCredentialsExpiredException expired(Throwable cause, String action) {
        S3Exception s3 = findCredentialsExpired(cause);
        if (s3 == null) {
            return null;
        }
        return new ExternalCredentialsExpiredException(
            cause,
            "Session credentials expired or invalid {}. Refresh the data source credentials and re-run the query. ({})",
            action,
            of(s3)
        );
    }

    static String of(Throwable cause) {
        if (cause instanceof S3Exception s3) {
            String code = s3.awsErrorDetails() != null ? s3.awsErrorDetails().errorCode() : null;
            return code == null || code.isEmpty() ? "HTTP " + s3.statusCode() : "HTTP " + s3.statusCode() + " " + code;
        }
        // Falls back to the class name so a null-message fault reads as its type rather than as the literal "null".
        return cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    }
}
