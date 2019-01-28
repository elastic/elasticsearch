/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;

import java.util.Collections;

public class GetDataFrameAnalyticsAction extends Action<GetDataFrameAnalyticsAction.Response> {

    public static final GetDataFrameAnalyticsAction INSTANCE = new GetDataFrameAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/get";

    private GetDataFrameAnalyticsAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response(new QueryPage<>(Collections.emptyList(), 0, Response.RESULTS_FIELD));
    }

    public static class Request extends AbstractGetResourcesRequest {

        @Override
        public String getResourceIdField() {
            return DataFrameAnalyticsConfig.ID.getPreferredName();
        }
    }

    public static class Response extends AbstractGetResourcesResponse<DataFrameAnalyticsConfig> {

        public static final ParseField RESULTS_FIELD = new ParseField("data_frame_analytics");

        public Response(QueryPage<DataFrameAnalyticsConfig> analytics) {
            super(analytics);
        }

        @Override
        protected Reader<DataFrameAnalyticsConfig> getReader() {
            return DataFrameAnalyticsConfig::new;
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        public RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }
}
