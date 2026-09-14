/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.services.s3.model.S3Exception;

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

    private S3FailureDetail() {}

    static String of(Throwable cause) {
        if (cause instanceof S3Exception s3) {
            String code = s3.awsErrorDetails() != null ? s3.awsErrorDetails().errorCode() : null;
            return code == null || code.isEmpty() ? "HTTP " + s3.statusCode() : "HTTP " + s3.statusCode() + " " + code;
        }
        // Falls back to the class name so a null-message fault reads as its type rather than as the literal "null".
        return cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    }

    /**
     * Redacts an S3 path for user-facing messages: keeps scheme and bucket, drops the object
     * prefix (which may embed the region, an account id, and partition values) down to the final
     * segment. The unreduced path stays available to server logs via {@link #log}.
     */
    static String redactPath(org.elasticsearch.xpack.esql.datasources.spi.StoragePath path) {
        if (path == null) {
            return "";
        }
        return path.scheme() + "://" + path.host() + "/" + path.objectName();
    }

    /**
     * Redacts an S3 listing prefix the same way: keeps the last path segment only, dropping
     * partition/prefix folders that can encode tenant details.
     */
    static String redactPrefix(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return "";
        }
        int lastSlash = prefix.lastIndexOf('/');
        return lastSlash >= 0 ? prefix.substring(lastSlash + 1) : prefix;
    }

    /**
     * Renders the full S3-side fault for the server log: the HTTP status, the provider error code and
     * message, and the request coordinates the SDK collected. Only for logging — never user-facing.
     */
    static String log(Throwable cause) {
        if (cause instanceof S3Exception s3) {
            StringBuilder b = new StringBuilder("HTTP ").append(s3.statusCode());
            if (s3.awsErrorDetails() != null) {
                if (s3.awsErrorDetails().errorCode() != null) {
                    b.append(" ").append(s3.awsErrorDetails().errorCode());
                }
                if (s3.awsErrorDetails().errorMessage() != null) {
                    b.append(": ").append(s3.awsErrorDetails().errorMessage());
                }
                if (s3.requestId() != null) {
                    b.append(" (requestId=").append(s3.requestId())
                        .append(", extendedRequestId=").append(s3.extendedRequestId()).append(')');
                }
            }
            return b.toString();
        }
        return of(cause);
    }

}
