/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class DocsStats implements Writeable, ToXContentFragment {

    private long count = 0;
    private long deleted = 0;
    private long totalSizeInBytes = 0;
    @Nullable
    private IgnoredFieldStats ignoredFieldStats;

    public DocsStats() {

    }

    public DocsStats(StreamInput in) throws IOException {
        count = in.readVLong();
        deleted = in.readVLong();
        totalSizeInBytes = in.readVLong();
        ignoredFieldStats = in.readOptionalWriteable(IgnoredFieldStats::new);
    }

    public DocsStats(long count, long deleted, long totalSizeInBytes, @Nullable final IgnoredFieldStats ignoredFieldStats) {
        this.count = count;
        this.deleted = deleted;
        this.totalSizeInBytes = totalSizeInBytes;
        this.ignoredFieldStats = ignoredFieldStats;
    }

    public void add(DocsStats other) {
        if (other == null) {
            return;
        }
        if (this.totalSizeInBytes == -1) {
            this.totalSizeInBytes = other.totalSizeInBytes;
        } else if (other.totalSizeInBytes != -1) {
            this.totalSizeInBytes += other.totalSizeInBytes;
        }
        this.count += other.count;
        this.deleted += other.deleted;
        if (ignoredFieldStats == null) {
            if (other.ignoredFieldStats != null) {
                ignoredFieldStats = new IgnoredFieldStats();
                ignoredFieldStats.add(other.ignoredFieldStats);
            }
        } else {
            ignoredFieldStats.add(other.ignoredFieldStats);
        }
    }

    public long getCount() {
        return this.count;
    }

    public long getDeleted() {
        return this.deleted;
    }

    /**
     * Returns the total size in bytes of all documents in this stats.
     * This value may be more reliable than {@link StoreStats#sizeInBytes()} in estimating the index size.
     */
    public long getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    @Nullable
    public IgnoredFieldStats getIgnoredFieldStats() {
        return ignoredFieldStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeVLong(deleted);
        out.writeVLong(totalSizeInBytes);
        if (out.getTransportVersion().onOrAfter(TransportVersions.IGNORED_FIELDS_STATS)) {
            out.writeOptionalWriteable(ignoredFieldStats);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DOCS);
        builder.field(Fields.COUNT, count);
        builder.field(Fields.DELETED, deleted);
        builder.field(Fields.TOTAL_SIZE_IN_BYTES, totalSizeInBytes);
        if (ignoredFieldStats != null) {
            ignoredFieldStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DocsStats that = (DocsStats) o;
        return count == that.count
            && deleted == that.deleted
            && totalSizeInBytes == that.totalSizeInBytes
            && Objects.equals(ignoredFieldStats, that.ignoredFieldStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, deleted, totalSizeInBytes, ignoredFieldStats);
    }

    static final class Fields {
        static final String DOCS = "docs";
        static final String COUNT = "count";
        static final String DELETED = "deleted";
        static final String TOTAL_SIZE_IN_BYTES = "total_size_in_bytes";
    }
}
