/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;

public final class ValueSources {

    private ValueSources() {}

    public static List<ValueSourceInfo> sources(List<SearchContext> searchContexts, String fieldName) {
        List<ValueSourceInfo> sources = new ArrayList<>(searchContexts.size());

        for (SearchContext searchContext : searchContexts) {
            SearchExecutionContext ctx = searchContext.getSearchExecutionContext();
            // TODO: should the missing fields be skipped if there's no mapping?
            var fieldType = ctx.getFieldType(fieldName);
            var fieldData = ctx.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
            var fieldContext = new FieldContext(fieldName, fieldData, fieldType);
            var vsType = fieldData.getValuesSourceType();
            var vs = vsType.getField(fieldContext, null);
            sources.add(new ValueSourceInfo(vsType, vs, ctx.getIndexReader()));
        }

        return sources;
    }
}
