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

package org.elasticsearch.client.dataframe;

import org.elasticsearch.client.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class GetDataFrameTransformStatsResponse {

    public static final ParseField TRANSFORMS = new ParseField("transforms");
    public static final ParseField COUNT = new ParseField("count");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GetDataFrameTransformStatsResponse, Void> PARSER = new ConstructingObjectParser<>(
            "get_data_frame_transform_stats_response", true, args -> new GetDataFrameTransformStatsResponse(
            (List<DataFrameTransformStateAndStats>) args[0]));

    static {
        PARSER.declareObjectArray(constructorArg(), DataFrameTransformStateAndStats.PARSER::apply, TRANSFORMS);
        // Discard the count field which is the size of the transforms array
        PARSER.declareInt((a, b) -> {}, COUNT);
    }

    public static GetDataFrameTransformStatsResponse fromXContent(final XContentParser parser) {
        return GetDataFrameTransformStatsResponse.PARSER.apply(parser, null);
    }

    private List<DataFrameTransformStateAndStats> transformsStateAndStats;

    public GetDataFrameTransformStatsResponse(List<DataFrameTransformStateAndStats> transformsStateAndStats) {
        this.transformsStateAndStats = transformsStateAndStats;
    }

    public List<DataFrameTransformStateAndStats> getTransformsStateAndStats() {
        return transformsStateAndStats;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformsStateAndStats);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final GetDataFrameTransformStatsResponse that = (GetDataFrameTransformStatsResponse) other;
        return Objects.equals(this.transformsStateAndStats, that.transformsStateAndStats);
    }
}
