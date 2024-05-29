/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DataExtractorQueryContext {

    public final String[] indices;
    public final QueryBuilder query;
    public final String timeField;
    public final long start;
    public final long end;
    public final Map<String, String> headers;
    public final IndicesOptions indicesOptions;
    public final Map<String, Object> runtimeMappings;

    public DataExtractorQueryContext(
        List<String> indices,
        QueryBuilder query,
        String timeField,
        long start,
        long end,
        Map<String, String> headers,
        IndicesOptions indicesOptions,
        Map<String, Object> runtimeMappings
    ) {
        this.indices = indices.toArray(new String[0]);
        this.query = Objects.requireNonNull(query);
        this.timeField = timeField;
        this.start = start;
        this.end = end;
        this.headers = headers;
        this.indicesOptions = Objects.requireNonNull(indicesOptions);
        this.runtimeMappings = Objects.requireNonNull(runtimeMappings);
    }
}
