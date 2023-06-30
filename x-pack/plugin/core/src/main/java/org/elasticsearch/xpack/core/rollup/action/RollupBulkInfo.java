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
 * This class includes statistics collected by the downsampling task
 * for bulk indexing operations.
 */
public record RollupBulkInfo(
    long totalBulkCount,
    long bulkIngestSumMillis,
    long maxBulkIngestMillis,
    long minBulkIngestMillis,
    long bulkTookSumMillis,
    long maxBulkTookMillis,
    long minBulkTookMillis
) implements NamedWriteable, ToXContentObject {

    public static final String NAME = "rollup_bulk_info";

    private static final ParseField TOTAL_BULK_COUNT = new ParseField("total_bulk_count");
    private static final ParseField BULK_INGEST_SUM_MILLIS = new ParseField("bulk_ingest_sum_millis");
    private static final ParseField MAX_BULK_INGEST_MILLIS = new ParseField("max_bulk_ingest_millis");
    private static final ParseField MIN_BULK_INGEST_MILLIS = new ParseField("min_bulk_ingest_millis");
    private static final ParseField BULK_TOOK_SUM_MILLIS = new ParseField("bulk_took_sum_millis");
    private static final ParseField MAX_BULK_TOOK_MILLIS = new ParseField("max_bulk_took_millis");
    private static final ParseField MIN_BULK_TOOK_MILLIS = new ParseField("min_bulk_took_millis");

    private static final ConstructingObjectParser<RollupBulkInfo, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(
            NAME,
            args -> new RollupBulkInfo(
                (Long) args[0],
                (Long) args[1],
                (Long) args[2],
                (Long) args[3],
                (Long) args[4],
                (Long) args[5],
                (Long) args[6]
            )
        );

        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_BULK_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), BULK_INGEST_SUM_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MAX_BULK_INGEST_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MIN_BULK_INGEST_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), BULK_TOOK_SUM_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MAX_BULK_TOOK_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MIN_BULK_TOOK_MILLIS);
    }

    public RollupBulkInfo(final StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
    }

    public static RollupBulkInfo fromXContext(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(TOTAL_BULK_COUNT.getPreferredName(), totalBulkCount);
        builder.field(BULK_INGEST_SUM_MILLIS.getPreferredName(), bulkIngestSumMillis);
        builder.field(MAX_BULK_INGEST_MILLIS.getPreferredName(), maxBulkIngestMillis);
        builder.field(MIN_BULK_INGEST_MILLIS.getPreferredName(), minBulkIngestMillis);
        builder.field(BULK_TOOK_SUM_MILLIS.getPreferredName(), bulkTookSumMillis);
        builder.field(MAX_BULK_TOOK_MILLIS.getPreferredName(), maxBulkTookMillis);
        builder.field(MIN_BULK_TOOK_MILLIS.getPreferredName(), minBulkTookMillis);
        return builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalBulkCount);
        out.writeVLong(bulkIngestSumMillis);
        out.writeVLong(maxBulkIngestMillis);
        out.writeVLong(minBulkIngestMillis);
        out.writeVLong(bulkTookSumMillis);
        out.writeVLong(maxBulkTookMillis);
        out.writeVLong(minBulkTookMillis);
    }
}
