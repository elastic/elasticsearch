/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;


public class GetFiltersAction extends Action<GetFiltersAction.Request, GetFiltersAction.Response, GetFiltersAction.RequestBuilder> {

    public static final GetFiltersAction INSTANCE = new GetFiltersAction();
    public static final String NAME = "cluster:admin/xpack/ml/filters/get";

    private GetFiltersAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest {

        private String filterId;
        private PageParams pageParams;

        public Request() {
        }

        public void setFilterId(String filterId) {
            this.filterId = filterId;
        }

        public String getFilterId() {
            return filterId;
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        public void setPageParams(PageParams pageParams) {
            this.pageParams = pageParams;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (pageParams != null && filterId != null) {
                validationException = addValidationError("Params [" + PageParams.FROM.getPreferredName() +
                        ", " + PageParams.SIZE.getPreferredName() + "] are incompatible with ["
                        + MlFilter.ID.getPreferredName() + "]", validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            filterId = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(filterId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filterId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(filterId, other.filterId);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

        private QueryPage<MlFilter> filters;

        public Response(QueryPage<MlFilter> filters) {
            this.filters = filters;
        }

        public Response() {
        }

        public QueryPage<MlFilter> getFilters() {
            return filters;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            filters = new QueryPage<>(in, MlFilter::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            filters.writeTo(out);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            filters.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(filters);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(filters, other.filters);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

}

