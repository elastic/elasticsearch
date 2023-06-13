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

public class RollupBeforeBulkInfo implements NamedWriteable, ToXContentObject {
    public static final String NAME = "rollup_before_bulk";
    private final long currentTimeMillis;
    private final long executionId;
    private final long estimatedSizeInBytes;
    private final int numberOfActions;

    private static final ParseField CURRENT_TIME_IN_MILLIS = new ParseField("current_time_in_millis");
    private static final ParseField EXECUTION_ID = new ParseField("execution_id");
    private static final ParseField ESTIMATED_SIZE_IN_BYTES = new ParseField("estimated_size_in_bytes");
    private static final ParseField NUMBER_OF_ACTIONS = new ParseField("number_of_actions");

    private static final ConstructingObjectParser<RollupBeforeBulkInfo, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(
            NAME,
            args -> new RollupBeforeBulkInfo((Long) args[0], (Long) args[1], (Long) args[2], (Integer) args[3])
        );

        PARSER.declareLong(ConstructingObjectParser.constructorArg(), CURRENT_TIME_IN_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), EXECUTION_ID);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), ESTIMATED_SIZE_IN_BYTES);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_ACTIONS);
    }

    public RollupBeforeBulkInfo(final StreamInput in) throws IOException {
        currentTimeMillis = in.readLong();
        executionId = in.readLong();
        estimatedSizeInBytes = in.readLong();
        numberOfActions = in.readInt();
    }

    public RollupBeforeBulkInfo(long currentTimeMillis, long executionId, long estimatedSizeInBytes, int numberOfActions) {
        this.currentTimeMillis = currentTimeMillis;
        this.executionId = executionId;
        this.estimatedSizeInBytes = estimatedSizeInBytes;
        this.numberOfActions = numberOfActions;
    }

    public long getCurrentTimeMillis() {
        return currentTimeMillis;
    }

    public long getExecutionId() {
        return executionId;
    }

    public long getEstimatedSizeInBytes() {
        return estimatedSizeInBytes;
    }

    public long getNumberOfActions() {
        return numberOfActions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RollupBeforeBulkInfo that = (RollupBeforeBulkInfo) o;
        return currentTimeMillis == that.currentTimeMillis
            && executionId == that.executionId
            && estimatedSizeInBytes == that.estimatedSizeInBytes
            && numberOfActions == that.numberOfActions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentTimeMillis, executionId, estimatedSizeInBytes, numberOfActions);
    }

    @Override
    public String toString() {
        return "RollupBeforeBulkInfo{"
            + "currentTimeMillis="
            + currentTimeMillis
            + ", executionId="
            + executionId
            + ", estimatedSizeInBytes="
            + estimatedSizeInBytes
            + ", numberOfActions="
            + numberOfActions
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
        out.writeLong(estimatedSizeInBytes);
        out.writeInt(numberOfActions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(CURRENT_TIME_IN_MILLIS.getPreferredName(), currentTimeMillis);
        builder.field(EXECUTION_ID.getPreferredName(), executionId);
        builder.field(ESTIMATED_SIZE_IN_BYTES.getPreferredName(), estimatedSizeInBytes);
        builder.field(NUMBER_OF_ACTIONS.getPreferredName(), numberOfActions);
        return builder.endObject();
    }

    public static RollupBeforeBulkInfo fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
