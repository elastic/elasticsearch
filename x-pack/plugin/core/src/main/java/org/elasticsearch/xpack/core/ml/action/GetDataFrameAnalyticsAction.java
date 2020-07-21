/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesRequest;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;

import java.io.IOException;

public class GetDataFrameAnalyticsAction extends ActionType<GetDataFrameAnalyticsAction.Response> {

    public static final GetDataFrameAnalyticsAction INSTANCE = new GetDataFrameAnalyticsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/data_frame/analytics/get";

    private GetDataFrameAnalyticsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AbstractGetResourcesRequest {

        public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");

        public Request() {
            setAllowNoResources(true);
        }

        public Request(String id) {
            setResourceId(id);
            setAllowNoResources(true);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getResourceIdField() {
            return DataFrameAnalyticsConfig.ID.getPreferredName();
        }
    }

    public static class Response extends AbstractGetResourcesResponse<DataFrameAnalyticsConfig> {

        public static final ParseField RESULTS_FIELD = new ParseField("data_frame_analytics");

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(QueryPage<DataFrameAnalyticsConfig> analytics) {
            super(analytics);
        }

        @Override
        protected Reader<DataFrameAnalyticsConfig> getReader() {
            return DataFrameAnalyticsConfig::new;
        }
    }
}
