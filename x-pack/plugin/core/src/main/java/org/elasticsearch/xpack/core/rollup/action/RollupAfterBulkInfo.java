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

/**
 * This class includes statistics collected by the downsampling task after
 * a bulk indexing operation ends.
 */
public record RollupAfterBulkInfo(
    long currentTimeMillis,
    long executionId,
    long lastIngestTookInMillis,
    long lastTookInMillis,
    boolean hasFailures,
    int restStatusCode
) implements NamedWriteable, ToXContentObject {

    public static final String NAME = "rollup_after_bulk_info";

    private static final ParseField CURRENT_TIME_IN_MILLIS = new ParseField("current_time_in_millis");
    private static final ParseField EXECUTION_ID = new ParseField("execution_id");
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
                (Boolean) args[4],
                (Integer) args[5]
            )
        );

        PARSER.declareLong(ConstructingObjectParser.constructorArg(), CURRENT_TIME_IN_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), EXECUTION_ID);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_INGEST_TOOK_IN_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_TOOK_IN_MILLIS);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), HAS_FAILURES);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), REST_STATUS_CODE);
    }

    public RollupAfterBulkInfo(final StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readBoolean(), in.readVInt());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(currentTimeMillis);
        out.writeVLong(executionId);
        out.writeVLong(lastIngestTookInMillis);
        out.writeVLong(lastTookInMillis);
        out.writeBoolean(hasFailures);
        out.writeVInt(restStatusCode);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(CURRENT_TIME_IN_MILLIS.getPreferredName(), currentTimeMillis);
        builder.field(EXECUTION_ID.getPreferredName(), executionId);
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
