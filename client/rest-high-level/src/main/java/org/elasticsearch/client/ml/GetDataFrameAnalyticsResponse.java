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

import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class GetDataFrameAnalyticsResponse {

    public static final ParseField DATA_FRAME_ANALYTICS = new ParseField("data_frame_analytics");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GetDataFrameAnalyticsResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "get_data_frame_analytics",
            true,
            args -> new GetDataFrameAnalyticsResponse((List<DataFrameAnalyticsConfig>) args[0]));

    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> DataFrameAnalyticsConfig.fromXContent(p), DATA_FRAME_ANALYTICS);
    }

    public static GetDataFrameAnalyticsResponse fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private List<DataFrameAnalyticsConfig> analytics;

    public GetDataFrameAnalyticsResponse(List<DataFrameAnalyticsConfig> analytics) {
        this.analytics = analytics;
    }

    public List<DataFrameAnalyticsConfig> getAnalytics() {
        return analytics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GetDataFrameAnalyticsResponse other = (GetDataFrameAnalyticsResponse) o;
        return Objects.equals(this.analytics, other.analytics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(analytics);
    }
}
