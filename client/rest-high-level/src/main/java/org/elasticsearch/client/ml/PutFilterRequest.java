/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.job.config.MlFilter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to create a new Machine Learning MlFilter given a {@link MlFilter} configuration
 */
public class PutFilterRequest implements Validatable, ToXContentObject {

    private final MlFilter filter;

    /**
     * Construct a new PutMlFilterRequest
     *
     * @param filter a {@link MlFilter} configuration to create
     */
    public PutFilterRequest(MlFilter filter) {
        this.filter = filter;
    }

    public MlFilter getMlFilter() {
        return filter;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return filter.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PutFilterRequest request = (PutFilterRequest) object;
        return Objects.equals(filter, request.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

}
