/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Base for leaf {@link StorageObject} implementations that meter their own I/O — the per-provider
 * readers (S3, GCS, Azure, HTTP, local). It owns the {@link StorageObjectMetricsCounters} instance
 * and the {@link #metrics()} snapshot accessor so the providers don't each re-declare the identical
 * field + method; subclasses call {@link #counters} from inside their own read paths to record
 * request timing and bytes.
 * <p>
 * Only the read-path metering itself is provider-specific (each SDK / transport surfaces byte
 * counts and completion differently), so {@code addRequest(...)} call sites stay in the subclasses.
 * <p>
 * Decorators (range, retry, concurrency-limit, …) do <b>not</b> extend this: they forward
 * {@link #metrics()} to their wrapped object so counters are attributed to the underlying store,
 * per the {@link StorageObject#metrics()} contract.
 */
public abstract class AbstractMeteredStorageObject implements StorageObject {

    /** Cumulative I/O counters for this object; subclasses record into it from their read paths. */
    protected final StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();

    @Override
    public final StorageObjectMetrics metrics() {
        return counters.snapshot();
    }
}
