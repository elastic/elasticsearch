/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class RepositoryStats implements Writeable {

    public static final RepositoryStats EMPTY_STATS = new RepositoryStats(Collections.emptyMap());

    public final Map<String, BlobStoreActionStats> actionStats;

    public RepositoryStats(Map<String, BlobStoreActionStats> actionStats) {
        this.actionStats = Collections.unmodifiableMap(actionStats);
    }

    public RepositoryStats(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.RETRIES_AND_OPERATIONS_IN_BLOBSTORE_STATS)) {
            this.actionStats = in.readMap(BlobStoreActionStats::new);
        } else {
            this.actionStats = in.readMap(si -> {
                long legacyValue = in.readLong();
                return new BlobStoreActionStats(legacyValue, legacyValue);
            });
        }
    }

    public RepositoryStats merge(RepositoryStats otherStats) {
        final Map<String, BlobStoreActionStats> result = new HashMap<>(actionStats);
        for (Map.Entry<String, BlobStoreActionStats> entry : otherStats.actionStats.entrySet()) {
            result.merge(entry.getKey(), entry.getValue(), BlobStoreActionStats::add);
        }
        return new RepositoryStats(result);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.RETRIES_AND_OPERATIONS_IN_BLOBSTORE_STATS)) {
            out.writeMap(actionStats, (so, v) -> v.writeTo(so));
        } else {
            out.writeMap(actionStats, (so, v) -> so.writeLong(v.requests()));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepositoryStats that = (RepositoryStats) o;
        return actionStats.equals(that.actionStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actionStats);
    }

    @Override
    public String toString() {
        return "RepositoryStats{actionStats=" + actionStats + '}';
    }
}
