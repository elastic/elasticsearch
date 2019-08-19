/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class EstimateMemoryUsageResponse implements ToXContentObject {
    
    public static final ParseField EXPECTED_MEMORY_USAGE_WITH_ONE_PARTITION =
        new ParseField("expected_memory_usage_with_one_partition");
    public static final ParseField EXPECTED_MEMORY_USAGE_WITH_MAX_PARTITIONS =
        new ParseField("expected_memory_usage_with_max_partitions");

    static final ConstructingObjectParser<EstimateMemoryUsageResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "estimate_memory_usage_response",
            true,
            args -> new EstimateMemoryUsageResponse((ByteSizeValue) args[0], (ByteSizeValue) args[1]));

    static {
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), EXPECTED_MEMORY_USAGE_WITH_ONE_PARTITION.getPreferredName()),
            EXPECTED_MEMORY_USAGE_WITH_ONE_PARTITION,
            ObjectParser.ValueType.VALUE);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), EXPECTED_MEMORY_USAGE_WITH_MAX_PARTITIONS.getPreferredName()),
            EXPECTED_MEMORY_USAGE_WITH_MAX_PARTITIONS,
            ObjectParser.ValueType.VALUE);
    }

    public static EstimateMemoryUsageResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final ByteSizeValue expectedMemoryUsageWithOnePartition;
    private final ByteSizeValue expectedMemoryUsageWithMaxPartitions;

    public EstimateMemoryUsageResponse(@Nullable ByteSizeValue expectedMemoryUsageWithOnePartition,
                                       @Nullable ByteSizeValue expectedMemoryUsageWithMaxPartitions) {
        this.expectedMemoryUsageWithOnePartition = expectedMemoryUsageWithOnePartition;
        this.expectedMemoryUsageWithMaxPartitions = expectedMemoryUsageWithMaxPartitions;
    }

    public ByteSizeValue getExpectedMemoryUsageWithOnePartition() {
        return expectedMemoryUsageWithOnePartition;
    }

    public ByteSizeValue getExpectedMemoryUsageWithMaxPartitions() {
        return expectedMemoryUsageWithMaxPartitions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (expectedMemoryUsageWithOnePartition != null) {
            builder.field(
                EXPECTED_MEMORY_USAGE_WITH_ONE_PARTITION.getPreferredName(), expectedMemoryUsageWithOnePartition.getStringRep());
        }
        if (expectedMemoryUsageWithMaxPartitions != null) {
            builder.field(
                EXPECTED_MEMORY_USAGE_WITH_MAX_PARTITIONS.getPreferredName(), expectedMemoryUsageWithMaxPartitions.getStringRep());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        EstimateMemoryUsageResponse that = (EstimateMemoryUsageResponse) other;
        return Objects.equals(expectedMemoryUsageWithOnePartition, that.expectedMemoryUsageWithOnePartition)
            && Objects.equals(expectedMemoryUsageWithMaxPartitions, that.expectedMemoryUsageWithMaxPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expectedMemoryUsageWithOnePartition, expectedMemoryUsageWithMaxPartitions);
    }
}
