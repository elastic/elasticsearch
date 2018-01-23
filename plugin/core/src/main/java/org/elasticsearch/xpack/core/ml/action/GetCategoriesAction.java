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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetCategoriesAction extends
Action<GetCategoriesAction.Request, GetCategoriesAction.Response, GetCategoriesAction.RequestBuilder> {

    public static final GetCategoriesAction INSTANCE = new GetCategoriesAction();
    public static final String NAME = "cluster:monitor/xpack/ml/job/results/categories/get";

    private GetCategoriesAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        public static final ParseField CATEGORY_ID = new ParseField("category_id");
        public static final ParseField FROM = new ParseField("from");
        public static final ParseField SIZE = new ParseField("size");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareLong(Request::setCategoryId, CATEGORY_ID);
            PARSER.declareObject(Request::setPageParams, PageParams.PARSER, PageParams.PAGE);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private String jobId;
        private Long categoryId;
        private PageParams pageParams;

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public Request() {
        }

        public String getJobId() { return jobId; }

        public PageParams getPageParams() { return pageParams; }

        public Long getCategoryId() { return categoryId; }

        public void setCategoryId(Long categoryId) {
            if (pageParams != null) {
                throw new IllegalArgumentException("Param [" + CATEGORY_ID.getPreferredName() + "] is incompatible with ["
                        + PageParams.FROM.getPreferredName() + ", " + PageParams.SIZE.getPreferredName() + "].");
            }
            this.categoryId = ExceptionsHelper.requireNonNull(categoryId, CATEGORY_ID.getPreferredName());
        }

        public void setPageParams(PageParams pageParams) {
            if (categoryId != null) {
                throw new IllegalArgumentException("Param [" + PageParams.FROM.getPreferredName() + ", "
                        + PageParams.SIZE.getPreferredName() + "] is incompatible with [" + CATEGORY_ID.getPreferredName() + "].");
            }
            this.pageParams = pageParams;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (pageParams == null && categoryId == null) {
                validationException = addValidationError("Both [" + CATEGORY_ID.getPreferredName() + "] and ["
                        + PageParams.FROM.getPreferredName() + ", " + PageParams.SIZE.getPreferredName() + "] "
                        + "cannot be null" , validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            categoryId = in.readOptionalLong();
            pageParams = in.readOptionalWriteable(PageParams::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeOptionalLong(categoryId);
            out.writeOptionalWriteable(pageParams);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            if (categoryId != null) {
                builder.field(CATEGORY_ID.getPreferredName(), categoryId);
            }
            if (pageParams != null) {
                builder.field(PageParams.PAGE.getPreferredName(), pageParams);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Request request = (Request) o;
            return Objects.equals(jobId, request.jobId)
                    && Objects.equals(categoryId, request.categoryId)
                    && Objects.equals(pageParams, request.pageParams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, categoryId, pageParams);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, GetCategoriesAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private QueryPage<CategoryDefinition> result;

        public Response(QueryPage<CategoryDefinition> result) {
            this.result = result;
        }

        public Response() {
        }

        public QueryPage<CategoryDefinition> getResult() {
            return result;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            result = new QueryPage<>(in, CategoryDefinition::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            result.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            result.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Response response = (Response) o;
            return Objects.equals(result, response.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(result);
        }
    }

}
