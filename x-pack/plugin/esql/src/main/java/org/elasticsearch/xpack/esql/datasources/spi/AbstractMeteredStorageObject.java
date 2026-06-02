/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Base for the leaf {@link StorageObject} providers (S3, GCS, Azure, HTTP, local): owns the
 * {@link #counters} field and the {@link #metrics()} accessor that each used to re-declare
 * identically. Subclasses record into {@link #counters} from their own read paths — the
 * byte-count extraction is provider-specific, so {@code addRequest} call sites stay in them.
 * Decorators do not extend this; they forward {@link #metrics()} to their delegate.
 */
public abstract class AbstractMeteredStorageObject implements StorageObject {

    protected final StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();

    @Override
    public final StorageObjectMetrics metrics() {
        return counters.snapshot();
    }
}
