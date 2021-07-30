/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.config.MlFilter;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Response containing the newly created {@link MlFilter}
 */
public class PutFilterResponse implements ToXContentObject {

    private MlFilter filter;

    public static PutFilterResponse fromXContent(XContentParser parser) throws IOException {
        return new PutFilterResponse(MlFilter.PARSER.parse(parser, null).build());
    }

    PutFilterResponse(MlFilter filter) {
        this.filter = filter;
    }

    public MlFilter getResponse() {
        return filter;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        filter.toXContent(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        PutFilterResponse response = (PutFilterResponse) object;
        return Objects.equals(filter, response.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter);
    }
}
