/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.explain;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class MemoryEstimation implements ToXContentObject, Writeable {

    public static final ParseField EXPECTED_MEMORY_WITHOUT_DISK = new ParseField("expected_memory_without_disk");
    public static final ParseField EXPECTED_MEMORY_WITH_DISK = new ParseField("expected_memory_with_disk");

    public static final ConstructingObjectParser<MemoryEstimation, Void> PARSER = new ConstructingObjectParser<>(
        "memory_estimation",
        a -> new MemoryEstimation((ByteSizeValue) a[0], (ByteSizeValue) a[1])
    );

    static {
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), EXPECTED_MEMORY_WITHOUT_DISK.getPreferredName()),
            EXPECTED_MEMORY_WITHOUT_DISK,
            ObjectParser.ValueType.VALUE
        );
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), EXPECTED_MEMORY_WITH_DISK.getPreferredName()),
            EXPECTED_MEMORY_WITH_DISK,
            ObjectParser.ValueType.VALUE
        );
    }

    private final ByteSizeValue expectedMemoryWithoutDisk;
    private final ByteSizeValue expectedMemoryWithDisk;

    public MemoryEstimation(@Nullable ByteSizeValue expectedMemoryWithoutDisk, @Nullable ByteSizeValue expectedMemoryWithDisk) {
        this.expectedMemoryWithoutDisk = expectedMemoryWithoutDisk;
        this.expectedMemoryWithDisk = expectedMemoryWithDisk;
    }

    public MemoryEstimation(StreamInput in) throws IOException {
        this.expectedMemoryWithoutDisk = in.readOptionalWriteable(ByteSizeValue::new);
        this.expectedMemoryWithDisk = in.readOptionalWriteable(ByteSizeValue::new);
    }

    public ByteSizeValue getExpectedMemoryWithoutDisk() {
        return expectedMemoryWithoutDisk;
    }

    public ByteSizeValue getExpectedMemoryWithDisk() {
        return expectedMemoryWithDisk;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(expectedMemoryWithoutDisk);
        out.writeOptionalWriteable(expectedMemoryWithDisk);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (expectedMemoryWithoutDisk != null) {
            builder.field(EXPECTED_MEMORY_WITHOUT_DISK.getPreferredName(), expectedMemoryWithoutDisk.getStringRep());
        }
        if (expectedMemoryWithDisk != null) {
            builder.field(EXPECTED_MEMORY_WITH_DISK.getPreferredName(), expectedMemoryWithDisk.getStringRep());
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

        MemoryEstimation that = (MemoryEstimation) other;
        return Objects.equals(expectedMemoryWithoutDisk, that.expectedMemoryWithoutDisk)
            && Objects.equals(expectedMemoryWithDisk, that.expectedMemoryWithDisk);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expectedMemoryWithoutDisk, expectedMemoryWithDisk);
    }
}
