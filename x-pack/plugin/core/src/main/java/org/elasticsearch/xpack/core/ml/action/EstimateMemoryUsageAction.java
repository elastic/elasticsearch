/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class EstimateMemoryUsageAction extends ActionType<EstimateMemoryUsageAction.Response> {

    public static final EstimateMemoryUsageAction INSTANCE = new EstimateMemoryUsageAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/estimate_memory_usage";

    private EstimateMemoryUsageAction() {
        super(NAME, EstimateMemoryUsageAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField TYPE = new ParseField("memory_usage_estimation_result");

        public static final ParseField EXPECTED_MEMORY_WITHOUT_DISK = new ParseField("expected_memory_without_disk");
        public static final ParseField EXPECTED_MEMORY_WITH_DISK = new ParseField("expected_memory_with_disk");

        static final ConstructingObjectParser<Response, Void> PARSER =
            new ConstructingObjectParser<>(
                TYPE.getPreferredName(),
                args -> new Response((ByteSizeValue) args[0], (ByteSizeValue) args[1]));

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

        public Response(@Nullable ByteSizeValue expectedMemoryWithoutDisk, @Nullable ByteSizeValue expectedMemoryWithDisk) {
            this.expectedMemoryWithoutDisk = expectedMemoryWithoutDisk;
            this.expectedMemoryWithDisk = expectedMemoryWithDisk;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
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

            Response that = (Response) other;
            return Objects.equals(expectedMemoryWithoutDisk, that.expectedMemoryWithoutDisk)
                && Objects.equals(expectedMemoryWithDisk, that.expectedMemoryWithDisk);
        }

        @Override
        public int hashCode() {
            return Objects.hash(expectedMemoryWithoutDisk, expectedMemoryWithDisk);
        }
    }
}
