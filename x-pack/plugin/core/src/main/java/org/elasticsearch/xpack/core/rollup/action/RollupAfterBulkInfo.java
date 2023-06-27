/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * This class includes statistics collected by the downsampling task after
 * a bulk indexing operation ends.
 */
public class RollupAfterBulkInfo implements NamedWriteable, ToXContentObject {
    public static final String NAME = "rollup_after_bulk_info";
    private final long currentTimeMillis;
    private final long executionId;
    private final long lastBulkDurationInMillis;
    private final long lastIngestTookInMillis;
    private final long lastTookInMillis;
    private final boolean hasFailures;
    private final int restStatusCode;

    private static final ParseField CURRENT_TIME_IN_MILLIS = new ParseField("current_time_in_millis");
    private static final ParseField EXECUTION_ID = new ParseField("execution_id");
    private static final ParseField LAST_BULK_DURATION_IN_MILLIS = new ParseField("last_bulk_duration_in_millis");
    private static final ParseField LAST_INGEST_TOOK_IN_MILLIS = new ParseField("last_ingest_took_in_millis");
    private static final ParseField LAST_TOOK_IN_MILLIS = new ParseField("last_took_in_millis");
    private static final ParseField HAS_FAILURES = new ParseField("has_failures");
    private static final ParseField REST_STATUS_CODE = new ParseField("rest_status_code");

    private static final ConstructingObjectParser<RollupAfterBulkInfo, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(
            NAME,
            args -> new RollupAfterBulkInfo(
                (Long) args[0],
                (Long) args[1],
                (Long) args[2],
                (Long) args[3],
                (Long) args[4],
                (Boolean) args[5],
                (Integer) args[6]
            )
        );

        PARSER.declareLong(ConstructingObjectParser.constructorArg(), CURRENT_TIME_IN_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), EXECUTION_ID);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_BULK_DURATION_IN_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_INGEST_TOOK_IN_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_TOOK_IN_MILLIS);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), HAS_FAILURES);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), REST_STATUS_CODE);
    }

    public RollupAfterBulkInfo(final StreamInput in) throws IOException {
        currentTimeMillis = in.readLong();
        executionId = in.readLong();
        lastBulkDurationInMillis = in.readLong();
        lastIngestTookInMillis = in.readLong();
        lastTookInMillis = in.readLong();
        hasFailures = in.readBoolean();
        restStatusCode = in.readInt();
    }

    public RollupAfterBulkInfo(
        long currentTimeMillis,
        long executionId,
        long bulkDurationInMillis,
        long ingestTookInMillis,
        long took,
        boolean hasFailures,
        int restStatusCode
    ) {
        this.currentTimeMillis = currentTimeMillis;
        this.executionId = executionId;
        this.lastBulkDurationInMillis = bulkDurationInMillis;
        this.lastIngestTookInMillis = ingestTookInMillis;
        this.lastTookInMillis = took;
        this.hasFailures = hasFailures;
        this.restStatusCode = restStatusCode;
    }

    public long getCurrentTimeMillis() {
        return currentTimeMillis;
    }

    public long getExecutionId() {
        return executionId;
    }

    public long getLastBulkDurationInMillis() {
        return lastBulkDurationInMillis;
    }

    public long getLastIngestTookInMillis() {
        return lastIngestTookInMillis;
    }

    public long getLastTookInMillis() {
        return lastTookInMillis;
    }

    public boolean hasFailures() {
        return hasFailures;
    }

    public int getRestStatusCode() {
        return restStatusCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RollupAfterBulkInfo that = (RollupAfterBulkInfo) o;
        return currentTimeMillis == that.currentTimeMillis
            && executionId == that.executionId
            && lastBulkDurationInMillis == that.lastBulkDurationInMillis
            && lastIngestTookInMillis == that.lastIngestTookInMillis
            && lastTookInMillis == that.lastTookInMillis
            && hasFailures == that.hasFailures
            && restStatusCode == that.restStatusCode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            currentTimeMillis,
            executionId,
            lastBulkDurationInMillis,
            lastIngestTookInMillis,
            lastTookInMillis,
            hasFailures,
            restStatusCode
        );
    }

    @Override
    public String toString() {
        return "RollupAfterBulkInfo{"
            + "currentTimeMillis="
            + currentTimeMillis
            + ", executionId="
            + executionId
            + ", bulkDurationInMillis="
            + lastBulkDurationInMillis
            + ", ingestTookInMillis="
            + lastIngestTookInMillis
            + ", took="
            + lastTookInMillis
            + ", hasFailures="
            + hasFailures
            + ", restStatusCode="
            + restStatusCode
            + '}';
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(currentTimeMillis);
        out.writeLong(executionId);
        out.writeLong(lastBulkDurationInMillis);
        out.writeLong(lastIngestTookInMillis);
        out.writeLong(lastTookInMillis);
        out.writeBoolean(hasFailures);
        out.writeInt(restStatusCode);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(CURRENT_TIME_IN_MILLIS.getPreferredName(), currentTimeMillis);
        builder.field(EXECUTION_ID.getPreferredName(), executionId);
        builder.field(LAST_BULK_DURATION_IN_MILLIS.getPreferredName(), lastBulkDurationInMillis);
        builder.field(LAST_INGEST_TOOK_IN_MILLIS.getPreferredName(), lastIngestTookInMillis);
        builder.field(LAST_TOOK_IN_MILLIS.getPreferredName(), lastTookInMillis);
        builder.field(HAS_FAILURES.getPreferredName(), hasFailures);
        builder.field(REST_STATUS_CODE.getPreferredName(), restStatusCode);
        return builder.endObject();
    }

    public static RollupAfterBulkInfo fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
