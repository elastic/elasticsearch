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
 * This class includes statistics collected by the downsampling task
 * for bulk indexing operations.
 */
public class RollupBulkInfo implements NamedWriteable, ToXContentObject {
    public static final String NAME = "rollup_bulk_info";

    private final long totalBulkCount;
    private final long bulkDurationSumMillis;
    private final long maxBulkDurationMillis;
    private final long minBulkDurationMillis;
    private final long bulkIngestSumMillis;
    private final long maxBulkIngestMillis;
    private final long minBulkIngestMillis;
    private final long bulkTookSumMillis;
    private final long maxBulkTookMillis;
    private final long minBulkTookMillis;

    private static final ParseField TOTAL_BULK_COUNT = new ParseField("total_bulk_count");
    private static final ParseField BULK_DURATION_SUM_MILLIS = new ParseField("bulk_duration_sum_millis");
    private static final ParseField MAX_BULK_DURATION_MILLIS = new ParseField("max_bulk_duration_millis");
    private static final ParseField MIN_BULK_DURATION_MILLIS = new ParseField("min_bulk_duration_millis");
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
                (Long) args[6],
                (Long) args[7],
                (Long) args[8],
                (Long) args[9]
            )
        );

        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_BULK_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), BULK_DURATION_SUM_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MAX_BULK_DURATION_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MIN_BULK_DURATION_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), BULK_INGEST_SUM_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MAX_BULK_INGEST_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MIN_BULK_INGEST_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), BULK_TOOK_SUM_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MAX_BULK_TOOK_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MIN_BULK_TOOK_MILLIS);
    }

    public RollupBulkInfo(final StreamInput in) throws IOException {
        this.totalBulkCount = in.readLong();
        this.bulkDurationSumMillis = in.readLong();
        this.maxBulkDurationMillis = in.readLong();
        this.minBulkDurationMillis = in.readLong();
        this.bulkIngestSumMillis = in.readLong();
        this.maxBulkIngestMillis = in.readLong();
        this.minBulkIngestMillis = in.readLong();
        this.bulkTookSumMillis = in.readLong();
        this.maxBulkTookMillis = in.readLong();
        this.minBulkTookMillis = in.readLong();
    }

    public RollupBulkInfo(
        long totalBulkCount,
        long bulkDurationSumMillis,
        long maxBulkDurationMillis,
        long minBulkDurationMillis,
        long bulkIngestSumMillis,
        long maxBulkIngestMillis,
        long minBulkIngestMillis,
        long bulkTookSumMillis,
        long maxBulkTookMillis,
        long minBulkTookMillis
    ) {
        this.totalBulkCount = totalBulkCount;
        this.bulkDurationSumMillis = bulkDurationSumMillis;
        this.maxBulkDurationMillis = maxBulkDurationMillis;
        this.minBulkDurationMillis = minBulkDurationMillis;
        this.bulkIngestSumMillis = bulkIngestSumMillis;
        this.maxBulkIngestMillis = maxBulkIngestMillis;
        this.minBulkIngestMillis = minBulkIngestMillis;
        this.bulkTookSumMillis = bulkTookSumMillis;
        this.maxBulkTookMillis = maxBulkTookMillis;
        this.minBulkTookMillis = minBulkTookMillis;
    }

    public static RollupBulkInfo fromXContext(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(TOTAL_BULK_COUNT.getPreferredName(), totalBulkCount);
        builder.field(BULK_DURATION_SUM_MILLIS.getPreferredName(), bulkDurationSumMillis);
        builder.field(MAX_BULK_DURATION_MILLIS.getPreferredName(), maxBulkDurationMillis);
        builder.field(MIN_BULK_DURATION_MILLIS.getPreferredName(), minBulkDurationMillis);
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
        out.writeLong(totalBulkCount);
        out.writeLong(bulkDurationSumMillis);
        out.writeLong(maxBulkDurationMillis);
        out.writeLong(minBulkDurationMillis);
        out.writeLong(bulkIngestSumMillis);
        out.writeLong(maxBulkIngestMillis);
        out.writeLong(minBulkIngestMillis);
        out.writeLong(bulkTookSumMillis);
        out.writeLong(maxBulkTookMillis);
        out.writeLong(minBulkTookMillis);
    }

    public long getTotalBulkCount() {
        return totalBulkCount;
    }

    public long getBulkDurationSumMillis() {
        return bulkDurationSumMillis;
    }

    public long getMaxBulkDurationMillis() {
        return maxBulkDurationMillis;
    }

    public long getMinBulkDurationMillis() {
        return minBulkDurationMillis;
    }

    public long getBulkIngestSumMillis() {
        return bulkIngestSumMillis;
    }

    public long getMaxBulkIngestMillis() {
        return maxBulkIngestMillis;
    }

    public long getMinBulkIngestMillis() {
        return minBulkIngestMillis;
    }

    public long getBulkTookSumMillis() {
        return bulkTookSumMillis;
    }

    public long getMaxBulkTookMillis() {
        return maxBulkTookMillis;
    }

    public long getMinBulkTookMillis() {
        return minBulkTookMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RollupBulkInfo that = (RollupBulkInfo) o;
        return totalBulkCount == that.totalBulkCount
            && bulkDurationSumMillis == that.bulkDurationSumMillis
            && maxBulkDurationMillis == that.maxBulkDurationMillis
            && minBulkDurationMillis == that.minBulkDurationMillis
            && bulkIngestSumMillis == that.bulkIngestSumMillis
            && maxBulkIngestMillis == that.maxBulkIngestMillis
            && minBulkIngestMillis == that.minBulkIngestMillis
            && bulkTookSumMillis == that.bulkTookSumMillis
            && maxBulkTookMillis == that.maxBulkTookMillis
            && minBulkTookMillis == that.minBulkTookMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            totalBulkCount,
            bulkDurationSumMillis,
            maxBulkDurationMillis,
            minBulkDurationMillis,
            bulkIngestSumMillis,
            maxBulkIngestMillis,
            minBulkIngestMillis,
            bulkTookSumMillis,
            maxBulkTookMillis,
            minBulkTookMillis
        );
    }

    @Override
    public String toString() {
        return "RollupBulkInfo{"
            + "totalBulkCount="
            + totalBulkCount
            + ", bulkDurationSumMillis="
            + bulkDurationSumMillis
            + ", maxBulkDurationMillis="
            + maxBulkDurationMillis
            + ", minBulkDurationMillis="
            + minBulkDurationMillis
            + ", bulkIngestSumMillis="
            + bulkIngestSumMillis
            + ", maxBulkIngestMillis="
            + maxBulkIngestMillis
            + ", minBulkIngestMillis="
            + minBulkIngestMillis
            + ", bulkTookSumMillis="
            + bulkTookSumMillis
            + ", maxBulkTookMillis="
            + maxBulkTookMillis
            + ", minBulkTookMillis="
            + minBulkTookMillis
            + '}';
    }
}
