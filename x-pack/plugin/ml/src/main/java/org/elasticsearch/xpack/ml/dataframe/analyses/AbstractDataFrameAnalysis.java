/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.analyses;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractDataFrameAnalysis implements DataFrameAnalysis {

    private static final String NAME = "name";
    private static final String PARAMETERS = "parameters";

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME, getType());
        builder.field(PARAMETERS, getParams());
        builder.endObject();
        return builder;
    }

    protected abstract Map<String, Object> getParams();
}
