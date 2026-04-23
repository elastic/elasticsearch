/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.telemetry.RecordingMeterRegistry;

@SuppressForbidden(reason = "use a http server")
public class GoogleCloudStorageTenaciousRetriesBlobContainerTests extends GoogleCloudStorageBlobContainerRetriesTests {

    final RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
    final RepositoriesMetrics repositoriesMetrics = new RepositoriesMetrics(recordingMeterRegistry);

    @Override
    protected BlobContainer createBlobContainer(
        final @Nullable Integer maxRetries,
        final @Nullable TimeValue readTimeout,
        final @Nullable TimeValue requestTimeout,
        final @Nullable Boolean disableChunkedEncoding,
        final @Nullable Integer maxConnections,
        final @Nullable ByteSizeValue bufferSize,
        final @Nullable Integer maxBulkDeletes,
        final @Nullable BlobPath blobContainerPath
    ) {
        BlobContainer delegate = super.createBlobContainer(
            maxRetries,
            readTimeout,
            requestTimeout,
            disableChunkedEncoding,
            maxConnections,
            bufferSize,
            maxBulkDeletes,
            blobContainerPath
        );

        return new GcsTenaciousRetryBlobContainer(delegate, repositoriesMetrics);
    }

    @Override
    public void testShouldRetryOnUnresolvableHost() {
        // TO DO
    }

    @Override
    public void testRetriesAreTerminatedWhenClientProviderIsClosed() {
        // TO DO
    }
}
