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

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class EstimateMemoryUsageRequest implements ToXContentObject, Validatable {

    private static final ParseField DATA_FRAME_ANALYTICS_CONFIG = new ParseField("data_frame_analytics_config");

    private static final ConstructingObjectParser<EstimateMemoryUsageRequest, Void> PARSER =
        new ConstructingObjectParser<>(
            "estimate_memory_usage_request",
            true,
            args -> {
                DataFrameAnalyticsConfig config = (DataFrameAnalyticsConfig) args[0];
                return new EstimateMemoryUsageRequest(config);
            });

    static {
        PARSER.declareObject(constructorArg(), (p, c) -> DataFrameAnalyticsConfig.fromXContent(p), DATA_FRAME_ANALYTICS_CONFIG);
    }

    public static EstimateMemoryUsageRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final DataFrameAnalyticsConfig config;

    public EstimateMemoryUsageRequest(DataFrameAnalyticsConfig config) {
        this.config = Objects.requireNonNull(config);
    }

    public DataFrameAnalyticsConfig getConfig() {
        return config;
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

        EstimateMemoryUsageRequest that = (EstimateMemoryUsageRequest) other;
        return Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }
}
