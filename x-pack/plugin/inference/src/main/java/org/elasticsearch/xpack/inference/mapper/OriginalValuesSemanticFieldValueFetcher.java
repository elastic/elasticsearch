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

import java.util.Map;
import java.util.Set;

class OriginalValuesSemanticFieldValueFetcher extends SourceValueFetcher {
    OriginalValuesSemanticFieldValueFetcher(String fieldName, SearchExecutionContext context) {
        super(fieldName, context);
    }

    OriginalValuesSemanticFieldValueFetcher(Set<String> sourcePaths, IgnoredSourceFieldMapper.IgnoredSourceFormat ignoredSourceFormat) {
        super(sourcePaths, null, ignoredSourceFormat);
    }

    @Override
    protected Object parseSourceValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            return map;
        } else {
            return value.toString();
        }
    }
}
