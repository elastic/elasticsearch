/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds the Data Lifecycle Management metadata that are configured in a data stream.
 */
public class DataLifecycleMetadata implements SimpleDiffable<DataLifecycleMetadata>, ToXContentFragment {

    public static final DataLifecycleMetadata EMPTY = new DataLifecycleMetadata();

    private static final ParseField DATA_RETENTION_FIELD = new ParseField("data_retention");
    private static final ParseField ROLLOVER_FIELD = new ParseField("rollover");

    private static final ConstructingObjectParser<DataLifecycleMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "data_lifecycle_metadata_parser",
        false,
        (args, unused) -> new DataLifecycleMetadata((TimeValue) args[0], (Rollover) args[1])
    );

    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), DATA_RETENTION_FIELD.getPreferredName()),
            DATA_RETENTION_FIELD,
            ObjectParser.ValueType.STRING_OR_NULL
        );
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> Rollover.fromXContent(p),
            ROLLOVER_FIELD
        );
    }

    @Nullable
    private final TimeValue dataRetention;
    @Nullable
    private final Rollover rollover;

    public DataLifecycleMetadata() {
        this(null, null);
    }

    public DataLifecycleMetadata(@Nullable TimeValue dataRetention) {
        this(dataRetention, null);
    }

    public DataLifecycleMetadata(@Nullable TimeValue dataRetention, Rollover rollover) {
        this.dataRetention = dataRetention;
        this.rollover = rollover;
    }

    public DataLifecycleMetadata(StreamInput in) throws IOException {
        this(in.readOptionalTimeValue(), in.readOptionalWriteable(Rollover::new));
    }

    @Nullable
    public TimeValue getDataRetention() {
        return dataRetention;
    }

    public static DataLifecycleMetadata addRolloverMetadata(DataLifecycleMetadata metadata, Rollover rollover) {
        return new DataLifecycleMetadata(metadata.dataRetention, rollover);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DataLifecycleMetadata that = (DataLifecycleMetadata) o;
        return Objects.equals(dataRetention, that.dataRetention)
            && Objects.equals(rollover, that.rollover);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataRetention, rollover);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalTimeValue(dataRetention);
        out.writeOptionalWriteable(rollover);
    }

    public static Diff<DataLifecycleMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataLifecycleMetadata::new, in);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (dataRetention != null) {
            builder.field(DATA_RETENTION_FIELD.getPreferredName(), dataRetention.getStringRep());
        }
        if (rollover != null) {
            builder.startObject(ROLLOVER_FIELD.getPreferredName());
            rollover.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    public static DataLifecycleMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Holds the rollover metadata that configure how data streams managed by Data Lifecycle Management get rolled over.
     */
    public static class Rollover implements SimpleDiffable<DataLifecycleMetadata>, ToXContentFragment {
        private static final ParseField MAX_AGE_FIELD = new ParseField("max_age");
        private static final ParseField MAX_PRIMARY_SHARD_SIZE_FIELD = new ParseField("max_primary_shard_size");
        private static final ParseField MIN_DOCS_FIELD = new ParseField("min_docs");

        private static final ConstructingObjectParser<Rollover, Void> PARSER = new ConstructingObjectParser<>(
            "data_lifecycle_rollover_parser",
            false,
            (args, unused) -> new Rollover((TimeValue) args[0], (ByteSizeValue) args[1], (Long) args[2])
        );

        static {
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), DATA_RETENTION_FIELD.getPreferredName()),
                DATA_RETENTION_FIELD,
                ObjectParser.ValueType.STRING_OR_NULL
            );
        }

        @Nullable
        private final TimeValue maxAge;
        @Nullable
        private final ByteSizeValue maxPrimaryShardSize;
        private final Long minDocs;

        public Rollover(@Nullable TimeValue maxAge, @Nullable ByteSizeValue maxPrimaryShardSize, Long minDocs) {
            if (maxAge == null && maxPrimaryShardSize == null) {
                throw new IllegalArgumentException(
                    "Rollover needs to have at least "
                        + MAX_AGE_FIELD.getPreferredName()
                        + " or "
                        + MAX_PRIMARY_SHARD_SIZE_FIELD.getPreferredName()
                        + " set."
                );
            }
            if (minDocs == null || minDocs > 0) {
                throw new IllegalArgumentException(MIN_DOCS_FIELD.getPreferredName() + " needs to be greater than 0.");

            }
            this.maxAge = maxAge;
            this.maxPrimaryShardSize = maxPrimaryShardSize;
            this.minDocs = minDocs;
        }

        public Rollover(StreamInput in) throws IOException {
            this(
                in.readOptionalTimeValue(),
                ByteSizeValue.parseBytesSizeValue(in.readOptionalString(), MAX_PRIMARY_SHARD_SIZE_FIELD.getPreferredName()),
                in.readVLong()
            );
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }

        public static Rollover fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (maxAge != null) {
                builder.field(MAX_AGE_FIELD.getPreferredName(), maxAge.getStringRep());
            }
            if (maxPrimaryShardSize != null) {
                builder.field(MAX_PRIMARY_SHARD_SIZE_FIELD.getPreferredName(), maxPrimaryShardSize.getStringRep());
            }
            builder.field(MIN_DOCS_FIELD.getPreferredName(), minDocs);
            return builder;
        }


        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalTimeValue(maxAge);
            out.writeOptionalString(maxPrimaryShardSize.getStringRep());
            out.writeVLong(minDocs);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final Rollover that = (Rollover) o;
            return Objects.equals(maxAge, that.maxAge)
                && Objects.equals(maxPrimaryShardSize, that.maxPrimaryShardSize)
                && Objects.equals(minDocs, that.minDocs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(maxAge, maxPrimaryShardSize, minDocs);
        }
    }
}
