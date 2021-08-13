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
import org.elasticsearch.client.ml.job.config.MlFilter;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A request to retrieve {@link MlFilter}s
 */
public class GetFiltersRequest implements Validatable, ToXContentObject {

    public static final ObjectParser<GetFiltersRequest, Void> PARSER =
        new ObjectParser<>("get_filters_request", GetFiltersRequest::new);

    static {
        PARSER.declareString(GetFiltersRequest::setFilterId, MlFilter.ID);
        PARSER.declareInt(GetFiltersRequest::setFrom, PageParams.FROM);
        PARSER.declareInt(GetFiltersRequest::setSize, PageParams.SIZE);
    }

    private String filterId;
    private Integer from;
    private Integer size;

    public String getFilterId() {
        return filterId;
    }

    public Integer getFrom() {
        return from;
    }

    public Integer getSize() {
        return size;
    }

    /**
     * Sets the filter id
     * @param filterId the filter id
     */
    public void setFilterId(String filterId) {
        this.filterId = filterId;
    }

    /**
     * Sets the number of filters to skip.
     * @param from set the `from` parameter
     */
    public void setFrom(Integer from) {
        this.from = from;
    }

    /**
     * Sets the number of filters to return.
     * @param size set the `size` parameter
     */
    public void setSize(Integer size) {
        this.size = size;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (filterId != null) {
            builder.field(MlFilter.ID.getPreferredName(), filterId);
        }
        if (from != null) {
            builder.field(PageParams.FROM.getPreferredName(), from);
        }
        if (size != null) {
            builder.field(PageParams.SIZE.getPreferredName(), size);
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
        GetFiltersRequest request = (GetFiltersRequest) obj;
        return Objects.equals(filterId, request.filterId)
            && Objects.equals(from, request.from)
            && Objects.equals(size, request.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filterId, from, size);
    }
}
