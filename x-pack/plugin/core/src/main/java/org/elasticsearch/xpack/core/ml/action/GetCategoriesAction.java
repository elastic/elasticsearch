/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetCategoriesAction extends ActionType<GetCategoriesAction.Response> {

    public static final GetCategoriesAction INSTANCE = new GetCategoriesAction();
    public static final String NAME = "cluster:monitor/xpack/ml/job/results/categories/get";

    private GetCategoriesAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        public static final ParseField CATEGORY_ID = CategoryDefinition.CATEGORY_ID;
        public static final ParseField FROM = new ParseField("from");
        public static final ParseField SIZE = new ParseField("size");
        public static final ParseField PARTITION_FIELD_VALUE = CategoryDefinition.PARTITION_FIELD_VALUE;

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareLong(Request::setCategoryId, CATEGORY_ID);
            PARSER.declareObject(Request::setPageParams, PageParams.PARSER, PageParams.PAGE);
            PARSER.declareString(Request::setPartitionFieldValue, PARTITION_FIELD_VALUE);
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
        private String partitionFieldValue;

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            categoryId = in.readOptionalLong();
            pageParams = in.readOptionalWriteable(PageParams::new);
            partitionFieldValue = in.readOptionalString();
        }

        public String getJobId() {
            return jobId;
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        public Long getCategoryId() {
            return categoryId;
        }

        public void setCategoryId(Long categoryId) {
            if (pageParams != null) {
                throw new IllegalArgumentException(
                    "Param ["
                        + CATEGORY_ID.getPreferredName()
                        + "] is incompatible with ["
                        + PageParams.FROM.getPreferredName()
                        + ", "
                        + PageParams.SIZE.getPreferredName()
                        + "]."
                );
            }
            this.categoryId = ExceptionsHelper.requireNonNull(categoryId, CATEGORY_ID.getPreferredName());
        }

        public void setPageParams(PageParams pageParams) {
            if (categoryId != null) {
                throw new IllegalArgumentException(
                    "Param ["
                        + PageParams.FROM.getPreferredName()
                        + ", "
                        + PageParams.SIZE.getPreferredName()
                        + "] is incompatible with ["
                        + CATEGORY_ID.getPreferredName()
                        + "]."
                );
            }
            this.pageParams = pageParams;
        }

        public String getPartitionFieldValue() {
            return partitionFieldValue;
        }

        public void setPartitionFieldValue(String partitionFieldValue) {
            this.partitionFieldValue = partitionFieldValue;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (pageParams == null && categoryId == null) {
                validationException = addValidationError(
                    "Both ["
                        + CATEGORY_ID.getPreferredName()
                        + "] and ["
                        + PageParams.FROM.getPreferredName()
                        + ", "
                        + PageParams.SIZE.getPreferredName()
                        + "] "
                        + "cannot be null",
                    validationException
                );
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeOptionalLong(categoryId);
            out.writeOptionalWriteable(pageParams);
            out.writeOptionalString(partitionFieldValue);
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
            if (partitionFieldValue != null) {
                builder.field(PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(jobId, request.jobId)
                && Objects.equals(categoryId, request.categoryId)
                && Objects.equals(pageParams, request.pageParams)
                && Objects.equals(partitionFieldValue, request.partitionFieldValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, categoryId, pageParams, partitionFieldValue);
        }
    }

    public static class Response extends AbstractGetResourcesResponse<CategoryDefinition> implements ToXContentObject {

        public Response(QueryPage<CategoryDefinition> result) {
            super(result);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public QueryPage<CategoryDefinition> getResult() {
            return getResources();
        }

        @Override
        protected Reader<CategoryDefinition> getReader() {
            return CategoryDefinition::new;
        }
    }

}
