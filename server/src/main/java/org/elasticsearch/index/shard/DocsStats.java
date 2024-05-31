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
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class DocsStats implements Writeable, ToXContentFragment {

    private long count = 0;
    private long deleted = 0;
    private long totalSizeInBytes = 0;
    private long docsWithIgnoredFields = -1;
    private long ignoredFieldTermsSumDocFreq = -1;

    public DocsStats() {

    }

    public DocsStats(StreamInput in) throws IOException {
        count = in.readVLong();
        deleted = in.readVLong();
        totalSizeInBytes = in.readVLong();
        if (in.getTransportVersion().onOrAfter(TransportVersions.IGNORED_FIELDS_STATS)) {
            docsWithIgnoredFields = in.readLong();
            ignoredFieldTermsSumDocFreq = in.readLong();
        }
    }

    public DocsStats(long count, long deleted, long totalSizeInBytes, long docsWithIgnoredFields, long ignoredFieldTermsSumDocFreq) {
        this.count = count;
        this.deleted = deleted;
        this.totalSizeInBytes = totalSizeInBytes;
        this.docsWithIgnoredFields = docsWithIgnoredFields;
        this.ignoredFieldTermsSumDocFreq = ignoredFieldTermsSumDocFreq;
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
        if (this.docsWithIgnoredFields == -1) {
            this.docsWithIgnoredFields = other.docsWithIgnoredFields;
        } else if (other.docsWithIgnoredFields != -1) {
            this.docsWithIgnoredFields += other.docsWithIgnoredFields;
        }
        if (this.ignoredFieldTermsSumDocFreq == -1) {
            this.ignoredFieldTermsSumDocFreq = other.ignoredFieldTermsSumDocFreq;
        } else if (other.ignoredFieldTermsSumDocFreq != -1) {
            this.ignoredFieldTermsSumDocFreq += other.ignoredFieldTermsSumDocFreq;
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

    public long getDocsWithIgnoredFields() {
        return docsWithIgnoredFields;
    }

    public long getIgnoredFieldTermsSumDocFreq() {
        return ignoredFieldTermsSumDocFreq;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeVLong(deleted);
        out.writeVLong(totalSizeInBytes);
        if (out.getTransportVersion().onOrAfter(TransportVersions.IGNORED_FIELDS_STATS)) {
            out.writeLong(docsWithIgnoredFields);
            out.writeLong(ignoredFieldTermsSumDocFreq);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DOCS);
        builder.field(Fields.COUNT, count);
        builder.field(Fields.DELETED, deleted);
        builder.field(Fields.TOTAL_SIZE_IN_BYTES, totalSizeInBytes);
        builder.field(Fields.DOCS_WITH_IGNORED_FIELDS, docsWithIgnoredFields);
        builder.field(Fields.SUM_DOC_FREQ_TERMS_IGNORED_FIELD, ignoredFieldTermsSumDocFreq);
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
            && docsWithIgnoredFields == that.docsWithIgnoredFields
            && ignoredFieldTermsSumDocFreq == that.ignoredFieldTermsSumDocFreq;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, deleted, totalSizeInBytes, docsWithIgnoredFields, ignoredFieldTermsSumDocFreq);
    }

    static final class Fields {
        static final String DOCS = "docs";
        static final String COUNT = "count";
        static final String DELETED = "deleted";
        static final String TOTAL_SIZE_IN_BYTES = "total_size_in_bytes";
        static final String DOCS_WITH_IGNORED_FIELDS = "docs_with_ignored_fields";
        static final String SUM_DOC_FREQ_TERMS_IGNORED_FIELD = "sum_doc_freq_terms_ignored_field";
    }
}
