/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Details about a specific {@link EvaluationMetric} that should be included in the response.
 */
public interface MetricDetail extends ToXContentObject, NamedWriteable {

    @Override
    default XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(getMetricName());
        innerToXContent(builder, params);
        builder.endObject();
        return builder.endObject();
    }

    default String getMetricName() {
        return getWriteableName();
    }

    /**
     * Implementations should write their own fields to the {@link XContentBuilder} passed in.
     */
    XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;
}
