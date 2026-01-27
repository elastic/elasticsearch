/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformProgress implements Writeable, ToXContentObject {

    public static final ParseField TOTAL_DOCS = new ParseField("total_docs");
    public static final ParseField DOCS_REMAINING = new ParseField("docs_remaining");
    public static final ParseField DOCS_PROCESSED = new ParseField("docs_processed");
    public static final ParseField DOCS_INDEXED = new ParseField("docs_indexed");
    public static final String PERCENT_COMPLETE = "percent_complete";

    public static final ConstructingObjectParser<TransformProgress, Void> PARSER = new ConstructingObjectParser<>(
        "data_frame_transform_progress",
        true,
        a -> new TransformProgress((Long) a[0], (Long) a[1], (Long) a[2], (Long) a[3])
    );

    static {
        PARSER.declareLong(optionalConstructorArg(), TOTAL_DOCS);
        PARSER.declareLong(optionalConstructorArg(), DOCS_REMAINING);
        PARSER.declareLong(optionalConstructorArg(), DOCS_PROCESSED);
        PARSER.declareLong(optionalConstructorArg(), DOCS_INDEXED);
    }

    private final Long totalDocs;
    private long documentsProcessed;
    private long documentsIndexed;

    public TransformProgress() {
        this(null, 0L, 0L);
    }

    // If we are reading from an old document we need to convert docsRemaining to docsProcessed
    public TransformProgress(Long totalDocs, Long docsRemaining, Long documentsProcessed, Long documentsIndexed) {
        this(
            totalDocs,
            documentsProcessed != null ? documentsProcessed : docsRemaining != null && totalDocs != null ? totalDocs - docsRemaining : 0L,
            documentsIndexed
        );
    }

    public TransformProgress(Long totalDocs, Long documentsProcessed, Long documentsIndexed) {
        if (totalDocs != null && totalDocs < 0) {
            throw new IllegalArgumentException("[total_docs] must be >0.");
        }
        this.totalDocs = totalDocs;
        if (documentsProcessed != null && documentsProcessed < 0) {
            throw new IllegalArgumentException("[docs_processed] must be >0.");
        }
        this.documentsProcessed = documentsProcessed == null ? 0 : documentsProcessed;
        if (documentsIndexed != null && documentsIndexed < 0) {
            throw new IllegalArgumentException("[docs_indexed] must be >0.");
        }
        this.documentsIndexed = documentsIndexed == null ? 0 : documentsIndexed;
    }

    public TransformProgress(TransformProgress otherProgress) {
        this.totalDocs = otherProgress.totalDocs;
        this.documentsProcessed = otherProgress.documentsProcessed;
        this.documentsIndexed = otherProgress.documentsIndexed;
    }

    public TransformProgress(StreamInput in) throws IOException {
        this.totalDocs = in.readOptionalLong();
        this.documentsProcessed = in.readVLong();
        this.documentsIndexed = in.readVLong();
    }

    public Double getPercentComplete() {
        if (totalDocs == null) {
            return null;
        }
        if (documentsProcessed >= totalDocs) {
            return 100.0;
        }
        return 100.0 * (double) documentsProcessed / totalDocs;
    }

    public Long getTotalDocs() {
        return totalDocs;
    }

    public void incrementDocsProcessed(long docsProcessed) {
        assert docsProcessed >= 0;
        this.documentsProcessed += docsProcessed;
    }

    public void incrementDocsIndexed(long numDocumentsIndexed) {
        assert numDocumentsIndexed >= 0;
        this.documentsIndexed += numDocumentsIndexed;
    }

    public long getDocumentsProcessed() {
        return documentsProcessed;
    }

    public long getDocumentsIndexed() {
        return documentsIndexed;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (other == null || other.getClass() != getClass()) {
            return false;
        }

        TransformProgress that = (TransformProgress) other;
        return Objects.equals(this.documentsIndexed, that.documentsIndexed)
            && Objects.equals(this.totalDocs, that.totalDocs)
            && Objects.equals(this.documentsProcessed, that.documentsProcessed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(documentsProcessed, documentsIndexed, totalDocs);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalLong(totalDocs);
        out.writeVLong(documentsProcessed);
        out.writeVLong(documentsIndexed);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (totalDocs != null) {
            builder.field(DOCS_REMAINING.getPreferredName(), documentsProcessed > totalDocs ? 0 : totalDocs - documentsProcessed);
            builder.field(TOTAL_DOCS.getPreferredName(), totalDocs);
            builder.field(PERCENT_COMPLETE, getPercentComplete());
        }
        builder.field(DOCS_INDEXED.getPreferredName(), documentsIndexed);
        builder.field(DOCS_PROCESSED.getPreferredName(), documentsProcessed);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
