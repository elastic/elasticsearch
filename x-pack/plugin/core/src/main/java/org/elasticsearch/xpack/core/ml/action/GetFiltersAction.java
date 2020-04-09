/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesRequest;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;

import java.io.IOException;


public class GetFiltersAction extends ActionType<GetFiltersAction.Response> {

    public static final GetFiltersAction INSTANCE = new GetFiltersAction();
    public static final String NAME = "cluster:admin/xpack/ml/filters/get";

    private GetFiltersAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AbstractGetResourcesRequest {

        public Request() {
            setAllowNoResources(true);
        }

        public Request(String filterId) {
            setResourceId(filterId);
            setAllowNoResources(true);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getResourceIdField() {
            return MlFilter.ID.getPreferredName();
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        public RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends AbstractGetResourcesResponse<MlFilter> implements StatusToXContentObject {

        public Response(QueryPage<MlFilter> filters) {
            super(filters);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public QueryPage<MlFilter> getFilters() {
            return getResources();
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        @Override
        protected Reader<MlFilter> getReader() {
            return MlFilter::new;
        }
    }

}

