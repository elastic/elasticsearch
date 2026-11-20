/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentType;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A hit that behaves like one parsed from a remote cluster: the source is a final field, so {@link #release()} counts the first call
 * and nothing else. A second call does nothing, as {@link PaginatedHitSource.Hit#release()} requires. Whoever still holds the hit still holds its source bytes, which is exactly why a finished page has to be
 * forgotten and not only released.
 */
final class RemoteStyleHit implements PaginatedHitSource.Hit {
    private final String index;
    private final String id;
    private final long version;
    private final BytesReference source;
    @Nullable
    private final Object[] sortValues;
    private final AtomicInteger releases;
    private final AtomicBoolean released = new AtomicBoolean();

    RemoteStyleHit(String index, String id, long version, BytesReference source, @Nullable Object[] sortValues, AtomicInteger releases) {
        this.index = index;
        this.id = id;
        this.version = version;
        this.source = source;
        this.sortValues = sortValues;
        this.releases = releases;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public long getSeqNo() {
        return 1L;
    }

    @Override
    public long getPrimaryTerm() {
        return 1L;
    }

    @Override
    public BytesReference getSource() {
        return source;
    }

    @Override
    public XContentType getXContentType() {
        return XContentType.JSON;
    }

    @Override
    public String getRouting() {
        return null;
    }

    @Override
    public Object[] getSortValues() {
        return sortValues;
    }

    @Override
    public void release() {
        if (released.compareAndSet(false, true)) {
            releases.incrementAndGet();
        }
    }
}
