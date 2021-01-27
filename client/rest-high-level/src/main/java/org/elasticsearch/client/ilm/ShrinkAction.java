/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
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
    private static final ParseField MAX_SINGLE_PRIMARY_SIZE = new ParseField("max_single_primary_size");

    private static final ConstructingObjectParser<ShrinkAction, Void> PARSER =
        new ConstructingObjectParser<>(NAME, true, a -> new ShrinkAction((Integer) a[0], (ByteSizeValue) a[1]));

    static {
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), NUMBER_OF_SHARDS_FIELD);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_SINGLE_PRIMARY_SIZE.getPreferredName()),
            MAX_SINGLE_PRIMARY_SIZE, ObjectParser.ValueType.STRING);
    }

    private Integer numberOfShards;
    private ByteSizeValue maxSinglePrimarySize;

    public static ShrinkAction parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public ShrinkAction(@Nullable Integer numberOfShards, ByteSizeValue maxSinglePrimarySize) {
        if (numberOfShards != null && maxSinglePrimarySize != null) {
            throw new IllegalArgumentException("Cannot set both [number_of_shards] and [max_single_primary_size]");
        }
        if (numberOfShards == null && maxSinglePrimarySize == null) {
            throw new IllegalArgumentException("Either [number_of_shards] or [max_single_primary_size] must be set");
        }
        if (maxSinglePrimarySize != null) {
            if (maxSinglePrimarySize.getBytes() <= 0) {
                throw new IllegalArgumentException("[max_single_primary_size] must be greater than 0");
            }
            this.maxSinglePrimarySize = maxSinglePrimarySize;
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

    ByteSizeValue getMaxSinglePrimarySize() {
        return maxSinglePrimarySize;
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
        if (maxSinglePrimarySize != null) {
            builder.field(MAX_SINGLE_PRIMARY_SIZE.getPreferredName(), maxSinglePrimarySize);
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
            Objects.equals(maxSinglePrimarySize, that.maxSinglePrimarySize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numberOfShards, maxSinglePrimarySize);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
