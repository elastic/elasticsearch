/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import com.google.cloud.storage.StorageException;

/**
 * Renders why a GCS call failed, for appending to the message of the {@link java.io.IOException} that carries it up
 * to ES|QL. The GCS counterpart of the S3 plugin's {@code S3FailureDetail}, and deliberately the same shape: the two
 * storage plugins should not describe the same class of failure differently.
 * <p>
 * It exists because naming only the operation and the path is not enough to act on: a refused connection, missing
 * credentials and a bucket the caller has no rights to all produce the same "&lt;operation&gt; failed for &lt;path&gt;",
 * and the part that says which one it was survives only in a nested cause that most clients never render.
 * <p>
 * For a {@link StorageException} that means the HTTP status plus the store's own reason ({@code forbidden},
 * {@code notFound}) rather than the SDK's full {@code toString}.
 */
final class GcsFailureDetail {

    private GcsFailureDetail() {}

    static String of(Throwable cause) {
        if (cause instanceof StorageException gcs) {
            String reason = gcs.getReason();
            return reason == null || reason.isEmpty() ? "HTTP " + gcs.getCode() : "HTTP " + gcs.getCode() + " " + reason;
        }
        // Falls back to the class name so a null-message fault reads as its type rather than as the literal "null".
        return cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    }
}
