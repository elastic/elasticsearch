/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.common.xcontent.ParseField;
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
