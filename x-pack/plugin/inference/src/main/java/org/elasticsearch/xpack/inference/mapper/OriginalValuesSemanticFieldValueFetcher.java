/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Set;

class OriginalValuesSemanticFieldValueFetcher extends SourceValueFetcher {
    private final String fieldName;

    OriginalValuesSemanticFieldValueFetcher(String fieldName, SearchExecutionContext context) {
        super(fieldName, context);
        this.fieldName = fieldName;
    }

    OriginalValuesSemanticFieldValueFetcher(
        String fieldName,
        Set<String> sourcePaths,
        IgnoredSourceFieldMapper.IgnoredSourceFormat ignoredSourceFormat
    ) {
        super(sourcePaths, null, ignoredSourceFormat);
        this.fieldName = fieldName;
    }

    @Override
    protected Object parseSourceValue(Object value) {
        // TODO: Can we simplify this?
        return SemanticTextUtils.nodeObjectValue(fieldName, value, true, false);
    }
}
