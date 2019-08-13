/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class EstimateMemoryUsageAction extends ActionType<EstimateMemoryUsageAction.Response> {

    public static final EstimateMemoryUsageAction INSTANCE = new EstimateMemoryUsageAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/estimate_memory_usage";

    private EstimateMemoryUsageAction() {
        super(NAME, EstimateMemoryUsageAction.Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        private static final ParseField DATA_FRAME_ANALYTICS_CONFIG = new ParseField("data_frame_analytics_config");

        private static final ConstructingObjectParser<EstimateMemoryUsageAction.Request, Void> PARSER =
            new ConstructingObjectParser<>(
                NAME,
                args -> {
                    DataFrameAnalyticsConfig.Builder configBuilder = (DataFrameAnalyticsConfig.Builder) args[0];
                    DataFrameAnalyticsConfig config = configBuilder.buildForMemoryEstimation();
                    return new EstimateMemoryUsageAction.Request(config);
                });

        static {
            PARSER.declareObject(constructorArg(), DataFrameAnalyticsConfig.STRICT_PARSER, DATA_FRAME_ANALYTICS_CONFIG);
        }

        public static EstimateMemoryUsageAction.Request parseRequest(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final DataFrameAnalyticsConfig config;

        public Request(DataFrameAnalyticsConfig config) {
            this.config = ExceptionsHelper.requireNonNull(config, DATA_FRAME_ANALYTICS_CONFIG);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new DataFrameAnalyticsConfig(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public DataFrameAnalyticsConfig getConfig() {
            return config;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            config.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DATA_FRAME_ANALYTICS_CONFIG.getPreferredName(), config);
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

            Request that = (Request) other;
            return Objects.equals(config, that.config);
        }

        @Override
        public int hashCode() {
            return Objects.hash(config);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField TYPE = new ParseField("memory_usage_estimation_result");

        public static final ParseField EXPECTED_MEMORY_USAGE_WITH_ONE_PARTITION =
            new ParseField("expected_memory_usage_with_one_partition");
        public static final ParseField EXPECTED_MEMORY_USAGE_WITH_MAX_PARTITIONS =
            new ParseField("expected_memory_usage_with_max_partitions");

        static final ConstructingObjectParser<Response, Void> PARSER =
            new ConstructingObjectParser<>(
                TYPE.getPreferredName(),
                args -> new Response((ByteSizeValue) args[0], (ByteSizeValue) args[1]));

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

        public Response(@Nullable ByteSizeValue expectedMemoryUsageWithOnePartition,
                        @Nullable ByteSizeValue expectedMemoryUsageWithMaxPartitions) {
            this.expectedMemoryUsageWithOnePartition = expectedMemoryUsageWithOnePartition;
            this.expectedMemoryUsageWithMaxPartitions = expectedMemoryUsageWithMaxPartitions;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.expectedMemoryUsageWithOnePartition = in.readOptionalWriteable(ByteSizeValue::new);
            this.expectedMemoryUsageWithMaxPartitions = in.readOptionalWriteable(ByteSizeValue::new);
        }

        public ByteSizeValue getExpectedMemoryUsageWithOnePartition() {
            return expectedMemoryUsageWithOnePartition;
        }

        public ByteSizeValue getExpectedMemoryUsageWithMaxPartitions() {
            return expectedMemoryUsageWithMaxPartitions;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(expectedMemoryUsageWithOnePartition);
            out.writeOptionalWriteable(expectedMemoryUsageWithMaxPartitions);
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

            Response that = (Response) other;
            return Objects.equals(expectedMemoryUsageWithOnePartition, that.expectedMemoryUsageWithOnePartition)
                && Objects.equals(expectedMemoryUsageWithMaxPartitions, that.expectedMemoryUsageWithMaxPartitions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(expectedMemoryUsageWithOnePartition, expectedMemoryUsageWithMaxPartitions);
        }
    }
}
