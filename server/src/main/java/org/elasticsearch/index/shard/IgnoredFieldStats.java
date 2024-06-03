/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class IgnoredFieldStats implements Writeable, ToXContentFragment {
    private long totalNumberOfDocuments = 0;
    private long docsWithIgnoredFields = 0;
    private long ignoredFieldTermsSumDocFreq = 0;

    public IgnoredFieldStats() {}

    public IgnoredFieldStats(long totalNumberOfDocuments, long docsWithIgnoredFields, long ignoredFieldTermsSumDocFreq) {
        this.totalNumberOfDocuments = totalNumberOfDocuments;
        this.docsWithIgnoredFields = docsWithIgnoredFields;
        this.ignoredFieldTermsSumDocFreq = ignoredFieldTermsSumDocFreq;
    }

    public IgnoredFieldStats(final StreamInput in) throws IOException {
        totalNumberOfDocuments = in.readVLong();
        docsWithIgnoredFields = in.readVLong();
        ignoredFieldTermsSumDocFreq = in.readVLong();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject(Fields.IGNORED_FIELD_STATS)
            .field(Fields.TOTAL_NUMBER_OF_DOCUMENTS, totalNumberOfDocuments)
            .field(Fields.DOCS_WITH_IGNORED_FIELDS, docsWithIgnoredFields)
            .field(Fields.SUM_DOC_FREQ_TERMS_IGNORED_FIELDS, ignoredFieldTermsSumDocFreq)
            .endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalNumberOfDocuments);
        out.writeVLong(docsWithIgnoredFields);
        out.writeVLong(ignoredFieldTermsSumDocFreq);
    }

    public long getTotalNumberOfDocuments() {
        return totalNumberOfDocuments;
    }

    public long getDocsWithIgnoredFields() {
        return docsWithIgnoredFields;
    }

    public long getIgnoredFieldTermsSumDocFreq() {
        return ignoredFieldTermsSumDocFreq;
    }

    public void add(final IgnoredFieldStats other) {
        if (other == null) {
            return;
        }

        totalNumberOfDocuments += other.totalNumberOfDocuments;
        docsWithIgnoredFields += other.docsWithIgnoredFields;
        ignoredFieldTermsSumDocFreq += other.ignoredFieldTermsSumDocFreq;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IgnoredFieldStats that = (IgnoredFieldStats) o;
        return totalNumberOfDocuments == that.totalNumberOfDocuments
            && docsWithIgnoredFields == that.docsWithIgnoredFields
            && ignoredFieldTermsSumDocFreq == that.ignoredFieldTermsSumDocFreq;
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalNumberOfDocuments, docsWithIgnoredFields, ignoredFieldTermsSumDocFreq);
    }

    static final class Fields {
        static final String IGNORED_FIELD_STATS = "ignored_field";
        static final String TOTAL_NUMBER_OF_DOCUMENTS = "total_docs";
        static final String DOCS_WITH_IGNORED_FIELDS = "docs_with_ignored_fields";
        static final String SUM_DOC_FREQ_TERMS_IGNORED_FIELDS = "sum_doc_freq_terms_ignored_fields";
    }
}
