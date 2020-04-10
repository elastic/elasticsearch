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
package org.elasticsearch.client.ml.dataframe.explain;

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

public class MemoryEstimation implements ToXContentObject {

    public static final ParseField EXPECTED_MEMORY_WITHOUT_DISK = new ParseField("expected_memory_without_disk");
    public static final ParseField EXPECTED_MEMORY_WITH_DISK = new ParseField("expected_memory_with_disk");

    public static final ConstructingObjectParser<MemoryEstimation, Void> PARSER = new ConstructingObjectParser<>("memory_estimation", true,
            a -> new MemoryEstimation((ByteSizeValue) a[0], (ByteSizeValue) a[1]));

    static {
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), EXPECTED_MEMORY_WITHOUT_DISK.getPreferredName()),
            EXPECTED_MEMORY_WITHOUT_DISK,
            ObjectParser.ValueType.VALUE);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), EXPECTED_MEMORY_WITH_DISK.getPreferredName()),
            EXPECTED_MEMORY_WITH_DISK,
            ObjectParser.ValueType.VALUE);
    }

    private final ByteSizeValue expectedMemoryWithoutDisk;
    private final ByteSizeValue expectedMemoryWithDisk;

    public MemoryEstimation(@Nullable ByteSizeValue expectedMemoryWithoutDisk, @Nullable ByteSizeValue expectedMemoryWithDisk) {
        this.expectedMemoryWithoutDisk = expectedMemoryWithoutDisk;
        this.expectedMemoryWithDisk = expectedMemoryWithDisk;
    }

    public ByteSizeValue getExpectedMemoryWithoutDisk() {
        return expectedMemoryWithoutDisk;
    }

    public ByteSizeValue getExpectedMemoryWithDisk() {
        return expectedMemoryWithDisk;
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
