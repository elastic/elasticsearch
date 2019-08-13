/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class MemoryUsageEstimationResult implements ToXContentObject {

    public static final ParseField TYPE = new ParseField("memory_usage_estimation_result");

    public static final ParseField EXPECTED_MEMORY_USAGE_WITH_ONE_PARTITION = new ParseField("expected_memory_usage_with_one_partition");
    public static final ParseField EXPECTED_MEMORY_USAGE_WITH_MAX_PARTITIONS = new ParseField("expected_memory_usage_with_max_partitions");

    public static final ConstructingObjectParser<MemoryUsageEstimationResult, Void> PARSER =
        new ConstructingObjectParser<>(
            TYPE.getPreferredName(),
            true,
            args -> new MemoryUsageEstimationResult((ByteSizeValue) args[0], (ByteSizeValue) args[1]));

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

    private final ByteSizeValue expectedMemoryUsageWithOnePartition;
    private final ByteSizeValue expectedMemoryUsageWithMaxPartitions;

    public MemoryUsageEstimationResult(@Nullable ByteSizeValue expectedMemoryUsageWithOnePartition,
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

        MemoryUsageEstimationResult that = (MemoryUsageEstimationResult) other;
        return Objects.equals(expectedMemoryUsageWithOnePartition, that.expectedMemoryUsageWithOnePartition)
            && Objects.equals(expectedMemoryUsageWithMaxPartitions, that.expectedMemoryUsageWithMaxPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expectedMemoryUsageWithOnePartition, expectedMemoryUsageWithMaxPartitions);
    }
}
