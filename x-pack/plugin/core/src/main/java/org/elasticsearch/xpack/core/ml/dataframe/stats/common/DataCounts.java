/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.common;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.dataframe.stats.Fields;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.Objects;

public class DataCounts implements ToXContentObject, Writeable {

    public static final String TYPE_VALUE = "analytics_data_counts";

    public static final ParseField TRAINING_DOCS_COUNT = new ParseField("training_docs_count");
    public static final ParseField TEST_DOCS_COUNT = new ParseField("test_docs_count");
    public static final ParseField SKIPPED_DOCS_COUNT = new ParseField("skipped_docs_count");

    public static final ConstructingObjectParser<DataCounts, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<DataCounts, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<DataCounts, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<DataCounts, Void> parser = new ConstructingObjectParser<>(
            TYPE_VALUE,
            ignoreUnknownFields,
            a -> new DataCounts((String) a[0], (long) a[1], (long) a[2], (long) a[3])
        );

        parser.declareString((bucket, s) -> {}, Fields.TYPE);
        parser.declareString(ConstructingObjectParser.constructorArg(), Fields.JOB_ID);
        parser.declareLong(ConstructingObjectParser.constructorArg(), TRAINING_DOCS_COUNT);
        parser.declareLong(ConstructingObjectParser.constructorArg(), TEST_DOCS_COUNT);
        parser.declareLong(ConstructingObjectParser.constructorArg(), SKIPPED_DOCS_COUNT);
        return parser;
    }

    private final String jobId;
    private final long trainingDocsCount;
    private final long testDocsCount;
    private final long skippedDocsCount;

    public DataCounts(String jobId) {
        this(jobId, 0, 0, 0);
    }

    public DataCounts(String jobId, long trainingDocsCount, long testDocsCount, long skippedDocsCount) {
        this.jobId = Objects.requireNonNull(jobId);
        this.trainingDocsCount = trainingDocsCount;
        this.testDocsCount = testDocsCount;
        this.skippedDocsCount = skippedDocsCount;
    }

    public DataCounts(StreamInput in) throws IOException {
        this.jobId = in.readString();
        this.trainingDocsCount = in.readVLong();
        this.testDocsCount = in.readVLong();
        this.skippedDocsCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeVLong(trainingDocsCount);
        out.writeVLong(testDocsCount);
        out.writeVLong(skippedDocsCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(Fields.TYPE.getPreferredName(), TYPE_VALUE);
            builder.field(Fields.JOB_ID.getPreferredName(), jobId);
        }
        builder.field(TRAINING_DOCS_COUNT.getPreferredName(), trainingDocsCount);
        builder.field(TEST_DOCS_COUNT.getPreferredName(), testDocsCount);
        builder.field(SKIPPED_DOCS_COUNT.getPreferredName(), skippedDocsCount);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataCounts that = (DataCounts) o;
        return Objects.equals(jobId, that.jobId)
            && trainingDocsCount == that.trainingDocsCount
            && testDocsCount == that.testDocsCount
            && skippedDocsCount == that.skippedDocsCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, trainingDocsCount, testDocsCount, skippedDocsCount);
    }

    public static String documentId(String jobId) {
        return TYPE_VALUE + "_" + jobId;
    }

    public String getJobId() {
        return jobId;
    }

    public long getTrainingDocsCount() {
        return trainingDocsCount;
    }

    public long getTestDocsCount() {
        return testDocsCount;
    }

    public long getSkippedDocsCount() {
        return skippedDocsCount;
    }
}
