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
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorQueryContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;

class ScrollDataExtractorContext {

    final String jobId;
    final TimeBasedExtractedFields extractedFields;
    final List<SearchSourceBuilder.ScriptField> scriptFields;
    final int scrollSize;
    final DataExtractorQueryContext queryContext;

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
        this.jobId = jobId;
        this.extractedFields = Objects.requireNonNull(extractedFields);
        this.scriptFields = Objects.requireNonNull(scriptFields);
        this.scrollSize = scrollSize;
        this.queryContext = new DataExtractorQueryContext(
            indices,
            query,
            extractedFields.timeField(),
            start,
            end,
            headers,
            indicesOptions,
            runtimeMappings
        );
    }
}
