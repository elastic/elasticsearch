/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameTransformProgress implements Writeable, ToXContentObject {

    private static final ParseField TOTAL_DOCS = new ParseField("total_docs");
    private static final ParseField DOCS_REMAINING = new ParseField("docs_remaining");
    private static final String PERCENT_COMPLETE = "percent_complete";

    public static final ConstructingObjectParser<DataFrameTransformProgress, Void> PARSER = new ConstructingObjectParser<>(
        "data_frame_transform_progress",
        true,
        a -> new DataFrameTransformProgress((Long) a[0], (Long)a[1]));

    static {
        PARSER.declareLong(constructorArg(), TOTAL_DOCS);
        PARSER.declareLong(optionalConstructorArg(), DOCS_REMAINING);
    }

    private final long totalDocs;
    private long remainingDocs;

    public DataFrameTransformProgress(long totalDocs, Long remainingDocs) {
        if (totalDocs < 0) {
            throw new IllegalArgumentException("[total_docs] must be >0.");
        }
        this.totalDocs = totalDocs;
        if (remainingDocs != null && remainingDocs < 0) {
            throw new IllegalArgumentException("[docs_remaining] must be >0.");
        }
        this.remainingDocs = remainingDocs == null ? totalDocs : remainingDocs;
    }

    public DataFrameTransformProgress(DataFrameTransformProgress otherProgress) {
        this.totalDocs = otherProgress.totalDocs;
        this.remainingDocs = otherProgress.remainingDocs;
    }

    public DataFrameTransformProgress(StreamInput in) throws IOException {
        this.totalDocs = in.readLong();
        this.remainingDocs = in.readLong();
    }

    public Double getPercentComplete() {
        if (totalDocs == 0) {
            return 100.0;
        }
        long docsRead = totalDocs - remainingDocs;
        if (docsRead < 0) {
            return 100.0;
        }
        return 100.0*(double)docsRead/totalDocs;
    }

    public long getTotalDocs() {
        return totalDocs;
    }

    public long getRemainingDocs() {
        return remainingDocs;
    }

    public void resetRemainingDocs() {
        this.remainingDocs = totalDocs;
    }

    public void docsProcessed(long docsProcessed) {
        assert docsProcessed >= 0;
        if (docsProcessed > remainingDocs) {
            remainingDocs = 0;
        } else {
            remainingDocs -= docsProcessed;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (other == null || other.getClass() != getClass()) {
            return false;
        }

        DataFrameTransformProgress that = (DataFrameTransformProgress) other;
        return Objects.equals(this.remainingDocs, that.remainingDocs) && Objects.equals(this.totalDocs, that.totalDocs);
    }

    @Override
    public int hashCode(){
        return Objects.hash(remainingDocs, totalDocs);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(totalDocs);
        out.writeLong(remainingDocs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TOTAL_DOCS.getPreferredName(), totalDocs);
        builder.field(DOCS_REMAINING.getPreferredName(), remainingDocs);
        builder.field(PERCENT_COMPLETE, getPercentComplete());
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
