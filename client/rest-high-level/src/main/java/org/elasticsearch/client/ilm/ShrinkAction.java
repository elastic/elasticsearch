/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class ShrinkAction implements LifecycleAction, ToXContentObject {
    public static final String NAME = "shrink";
    private static final ParseField NUMBER_OF_SHARDS_FIELD = new ParseField("number_of_shards");
    private static final ParseField MAX_PRIMARY_SHARD_SIZE = new ParseField("max_primary_shard_size");

    private static final ConstructingObjectParser<ShrinkAction, Void> PARSER =
        new ConstructingObjectParser<>(NAME, true, a -> new ShrinkAction((Integer) a[0], (ByteSizeValue) a[1]));

    static {
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), NUMBER_OF_SHARDS_FIELD);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_PRIMARY_SHARD_SIZE.getPreferredName()),
            MAX_PRIMARY_SHARD_SIZE, ObjectParser.ValueType.STRING);
    }

    private Integer numberOfShards;
    private ByteSizeValue maxPrimaryShardSize;

    public static ShrinkAction parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public ShrinkAction(@Nullable Integer numberOfShards, ByteSizeValue maxPrimaryShardSize) {
        if (numberOfShards != null && maxPrimaryShardSize != null) {
            throw new IllegalArgumentException("Cannot set both [number_of_shards] and [max_primary_shard_size]");
        }
        if (numberOfShards == null && maxPrimaryShardSize == null) {
            throw new IllegalArgumentException("Either [number_of_shards] or [max_primary_shard_size] must be set");
        }
        if (maxPrimaryShardSize != null) {
            if (maxPrimaryShardSize.getBytes() <= 0) {
                throw new IllegalArgumentException("[max_primary_shard_size] must be greater than 0");
            }
            this.maxPrimaryShardSize = maxPrimaryShardSize;
        } else {
            if (numberOfShards <= 0) {
                throw new IllegalArgumentException("[" + NUMBER_OF_SHARDS_FIELD.getPreferredName() + "] must be greater than 0");
            }
            this.numberOfShards = numberOfShards;
        }
    }

    Integer getNumberOfShards() {
        return numberOfShards;
    }

    ByteSizeValue getMaxPrimaryShardSize() {
        return maxPrimaryShardSize;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (numberOfShards != null) {
            builder.field(NUMBER_OF_SHARDS_FIELD.getPreferredName(), numberOfShards);
        }
        if (maxPrimaryShardSize != null) {
            builder.field(MAX_PRIMARY_SHARD_SIZE.getPreferredName(), maxPrimaryShardSize);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShrinkAction that = (ShrinkAction) o;

        return Objects.equals(numberOfShards, that.numberOfShards) &&
            Objects.equals(maxPrimaryShardSize, that.maxPrimaryShardSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numberOfShards, maxPrimaryShardSize);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
