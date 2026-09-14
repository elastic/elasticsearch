/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * What the numbers were measured on. A fixed {@code visit_percentage} means something different on one segment than on twenty, and
 * nothing about a recall figure is reproducible without the field's own configuration, so both travel with the result. Each block is
 * {@code null} when the call behind it was not permitted.
 */
public record KnnEvalEnvironment(
    @Nullable IndexSummary index,
    @Nullable FieldSummary field,
    List<String> indexVersionCreated,
    boolean allowExpensiveQueries
) implements Writeable, ToXContentObject {

    static final String INDEX_FIELD = "index";
    static final String FIELD_FIELD = "field";
    static final String INDEX_VERSION_CREATED_FIELD = "index_version_created";
    static final String ALLOW_EXPENSIVE_QUERIES_FIELD = "allow_expensive_queries";

    public KnnEvalEnvironment(
        @Nullable IndexSummary index,
        @Nullable FieldSummary field,
        List<String> indexVersionCreated,
        boolean allowExpensiveQueries
    ) {
        this.index = index;
        this.field = field;
        this.indexVersionCreated = List.copyOf(indexVersionCreated);
        this.allowExpensiveQueries = allowExpensiveQueries;
    }

    KnnEvalEnvironment(StreamInput in) throws IOException {
        this(
            in.readOptionalWriteable(IndexSummary::new),
            in.readOptionalWriteable(FieldSummary::new),
            in.readStringCollectionAsList(),
            in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(index);
        out.writeOptionalWriteable(field);
        out.writeStringCollection(indexVersionCreated);
        out.writeBoolean(allowExpensiveQueries);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (index != null) {
            builder.field(INDEX_FIELD);
            index.toXContent(builder, params);
        }
        if (field != null) {
            builder.field(FIELD_FIELD);
            field.toXContent(builder, params);
        }
        if (indexVersionCreated.size() == 1) {
            builder.field(INDEX_VERSION_CREATED_FIELD, indexVersionCreated.get(0));
        } else if (indexVersionCreated.isEmpty() == false) {
            // resolved indices were not all created on the same version, which is itself worth seeing
            builder.field(INDEX_VERSION_CREATED_FIELD, indexVersionCreated);
        }
        builder.field(ALLOW_EXPENSIVE_QUERIES_FIELD, allowExpensiveQueries);
        builder.endObject();
        return builder;
    }

    /** The segment layout a fixed {@code visit_percentage} or {@code num_candidates} was measured against. */
    public record IndexSummary(
        int indices,
        int shards,
        long liveDocs,
        long deletedDocs,
        int segmentCount,
        long minSegmentDocs,
        long medianSegmentDocs,
        long maxSegmentDocs,
        long storeSizeInBytes
    ) implements Writeable, ToXContentObject {

        IndexSummary(StreamInput in) throws IOException {
            this(
                in.readVInt(),
                in.readVInt(),
                in.readVLong(),
                in.readVLong(),
                in.readVInt(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(indices);
            out.writeVInt(shards);
            out.writeVLong(liveDocs);
            out.writeVLong(deletedDocs);
            out.writeVInt(segmentCount);
            out.writeVLong(minSegmentDocs);
            out.writeVLong(medianSegmentDocs);
            out.writeVLong(maxSegmentDocs);
            out.writeVLong(storeSizeInBytes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("indices", indices);
            builder.field("shards", shards);
            builder.startObject("docs");
            builder.field("live", liveDocs);
            builder.field("deleted", deletedDocs);
            builder.endObject();
            builder.startObject("segments");
            builder.field("count", segmentCount);
            builder.field("min_docs", minSegmentDocs);
            builder.field("median_docs", medianSegmentDocs);
            builder.field("max_docs", maxSegmentDocs);
            builder.endObject();
            builder.field("store_size_in_bytes", storeSizeInBytes);
            builder.endObject();
            return builder;
        }
    }

    /** The field's own configuration, echoed from its mapping; {@code indexOptions} is passed through whatever keys it has. */
    public record FieldSummary(
        String type,
        @Nullable Integer dims,
        String elementType,
        @Nullable String similarity,
        Map<String, Object> indexOptions
    ) implements Writeable, ToXContentObject {

        @SuppressWarnings("unchecked")
        FieldSummary(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readOptionalVInt(),
                in.readString(),
                in.readOptionalString(),
                (Map<String, Object>) in.readGenericValue()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(type);
            out.writeOptionalVInt(dims);
            out.writeString(elementType);
            out.writeOptionalString(similarity);
            out.writeGenericValue(indexOptions);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            if (dims != null) {
                builder.field("dims", dims);
            }
            builder.field("element_type", elementType);
            if (similarity != null) {
                builder.field("similarity", similarity);
            }
            if (indexOptions.isEmpty() == false) {
                builder.field("index_options", indexOptions);
            }
            builder.endObject();
            return builder;
        }
    }
}
