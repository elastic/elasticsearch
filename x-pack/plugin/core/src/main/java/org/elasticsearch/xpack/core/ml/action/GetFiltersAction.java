/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesRequest;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;

import java.io.IOException;

import static org.elasticsearch.core.Strings.format;

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
        public String getCancelableTaskDescription() {
            return format("get_filters[%s]", getResourceId());
        }

        @Override
        public String getResourceIdField() {
            return MlFilter.ID.getPreferredName();
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
