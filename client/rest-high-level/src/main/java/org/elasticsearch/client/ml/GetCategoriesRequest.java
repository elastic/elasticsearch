/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.results.CategoryDefinition;
import org.elasticsearch.common.ParseField;
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
