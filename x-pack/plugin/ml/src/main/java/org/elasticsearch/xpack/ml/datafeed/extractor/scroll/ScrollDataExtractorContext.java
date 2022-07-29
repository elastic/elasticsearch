/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Map;
import java.util.Objects;

class ScrollDataExtractorContext {

    final String jobId;
    final TimeBasedExtractedFields extractedFields;
    final String[] indices;
    final QueryBuilder query;
    final List<SearchSourceBuilder.ScriptField> scriptFields;
    final int scrollSize;
    final long start;
    final long end;
    final Map<String, String> headers;
    final IndicesOptions indicesOptions;
    final Map<String, Object> runtimeMappings;

    ScrollDataExtractorContext(
        String jobId,
        TimeBasedExtractedFields extractedFields,
        List<String> indices,
        QueryBuilder query,
        List<SearchSourceBuilder.ScriptField> scriptFields,
        int scrollSize,
        long start,
        long end,
        Map<String, String> headers,
        IndicesOptions indicesOptions,
        Map<String, Object> runtimeMappings
    ) {
        this.jobId = Objects.requireNonNull(jobId);
        this.extractedFields = Objects.requireNonNull(extractedFields);
        this.indices = indices.toArray(new String[indices.size()]);
        this.query = Objects.requireNonNull(query);
        this.scriptFields = Objects.requireNonNull(scriptFields);
        this.scrollSize = scrollSize;
        this.start = start;
        this.end = end;
        this.headers = headers;
        this.indicesOptions = Objects.requireNonNull(indicesOptions);
        this.runtimeMappings = Objects.requireNonNull(runtimeMappings);
    }
}
