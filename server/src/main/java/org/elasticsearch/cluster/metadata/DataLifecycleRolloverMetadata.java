/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

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
 * Holds the rollover metadata that configure how data streams managed by Data Lifecycle Management get rolled over.
 */
public class DataLifecycleRolloverMetadata implements SimpleDiffable<DataLifecycleMetadata>, ToXContentFragment {
    private static final ParseField MAX_AGE_FIELD = new ParseField("max_age");
    private static final ParseField MAX_PRIMARY_SHARD_SIZE_FIELD = new ParseField("max_primary_shard_size");
    private static final ParseField MIN_DOCS_FIELD = new ParseField("min_docs");

    private static final ConstructingObjectParser<DataLifecycleRolloverMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "data_lifecycle_rollover_parser",
        false,
        (args, unused) -> new DataLifecycleRolloverMetadata((TimeValue) args[0], (ByteSizeValue) args[1], (Long) args[2])
    );

    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), MAX_AGE_FIELD.getPreferredName()),
            MAX_AGE_FIELD,
            ObjectParser.ValueType.STRING_OR_NULL
        );
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.textOrNull(), MAX_PRIMARY_SHARD_SIZE_FIELD.getPreferredName()),
            MAX_PRIMARY_SHARD_SIZE_FIELD,
            ObjectParser.ValueType.STRING_OR_NULL
        );
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> p.longValue(),
            MIN_DOCS_FIELD,
            ObjectParser.ValueType.LONG
        );
    }

    @Nullable
    private final TimeValue maxAge;
    @Nullable
    private final ByteSizeValue maxPrimaryShardSize;
    private final Long minDocs;

    public DataLifecycleRolloverMetadata(@Nullable TimeValue maxAge, @Nullable ByteSizeValue maxPrimaryShardSize, Long minDocs) {
        if (maxAge == null && maxPrimaryShardSize == null) {
            throw new IllegalArgumentException(
                "Rollover needs to have at least "
                    + MAX_AGE_FIELD.getPreferredName()
                    + " or "
                    + MAX_PRIMARY_SHARD_SIZE_FIELD.getPreferredName()
                    + " set."
            );
        }
        if (minDocs == null || minDocs <= 0) {
            throw new IllegalArgumentException(MIN_DOCS_FIELD.getPreferredName() + " needs to be greater than 0.");

        }
        this.maxAge = maxAge;
        this.maxPrimaryShardSize = maxPrimaryShardSize;
        this.minDocs = minDocs;
    }

    public DataLifecycleRolloverMetadata(StreamInput in) throws IOException {
        this(
            in.readOptionalTimeValue(),
            ByteSizeValue.parseBytesSizeValue(in.readOptionalString(), MAX_PRIMARY_SHARD_SIZE_FIELD.getPreferredName()),
            in.readVLong()
        );
    }

    public TimeValue getMaxAge() {
        return maxAge;
    }

    public ByteSizeValue getMaxPrimaryShardSize() {
        return maxPrimaryShardSize;
    }

    public Long getMinDocs() {
        return minDocs;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static DataLifecycleRolloverMetadata fromXContent(XContentParser parser) throws IOException {
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
        out.writeOptionalString(maxPrimaryShardSize == null ? null : maxPrimaryShardSize.getStringRep());
        out.writeVLong(minDocs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DataLifecycleRolloverMetadata that = (DataLifecycleRolloverMetadata) o;
        return Objects.equals(maxAge, that.maxAge)
            && Objects.equals(maxPrimaryShardSize, that.maxPrimaryShardSize)
            && Objects.equals(minDocs, that.minDocs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxAge, maxPrimaryShardSize, minDocs);
    }
}
