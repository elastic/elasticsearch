/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.results.CategoryDefinition;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A request to retrieve categories of a given job
 */
public class GetCategoriesRequest implements Validatable, ToXContentObject {

    public static final ParseField CATEGORY_ID = CategoryDefinition.CATEGORY_ID;
    public static final ParseField PARTITION_FIELD_VALUE = CategoryDefinition.PARTITION_FIELD_VALUE;

    public static final ConstructingObjectParser<GetCategoriesRequest, Void> PARSER = new ConstructingObjectParser<>(
        "get_categories_request", a -> new GetCategoriesRequest((String) a[0]));


    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareLong(GetCategoriesRequest::setCategoryId, CATEGORY_ID);
        PARSER.declareObject(GetCategoriesRequest::setPageParams, PageParams.PARSER, PageParams.PAGE);
        PARSER.declareString(GetCategoriesRequest::setPartitionFieldValue, PARTITION_FIELD_VALUE);
    }

    private final String jobId;
    private Long categoryId;
    private PageParams pageParams;
    private String partitionFieldValue;

    /**
     * Constructs a request to retrieve category information from a given job
     * @param jobId id of the job from which to retrieve results
     */
    public GetCategoriesRequest(String jobId) {
        this.jobId = Objects.requireNonNull(jobId);
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

    /**
     * Sets the category id
     * @param categoryId the category id
     */
    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    /**
     * Sets the paging parameters
     * @param pageParams the paging parameters
     */
    public void setPageParams(PageParams pageParams) {
        this.pageParams = pageParams;
    }

    public String getPartitionFieldValue() {
        return partitionFieldValue;
    }

    /**
     * Sets the partition field value
     * @param partitionFieldValue the partition field value
     */
    public void setPartitionFieldValue(String partitionFieldValue) {
        this.partitionFieldValue = partitionFieldValue;
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
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetCategoriesRequest request = (GetCategoriesRequest) obj;
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
