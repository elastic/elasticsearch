/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.StoredFieldSortedBinaryIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.TextDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;

public final class ValueSources {

    private ValueSources() {}

    public static List<ValueSourceInfo> sources(
        List<SearchContext> searchContexts,
        String fieldName,
        boolean asUnsupportedSource,
        ElementType elementType
    ) {
        List<ValueSourceInfo> sources = new ArrayList<>(searchContexts.size());

        for (SearchContext searchContext : searchContexts) {
            SearchExecutionContext ctx = searchContext.getSearchExecutionContext();
            var fieldType = ctx.getFieldType(fieldName);
            if (fieldType == null) {
                sources.add(new ValueSourceInfo(new NullValueSourceType(), new NullValueSource(), elementType, ctx.getIndexReader()));
                continue; // the field does not exist in this context
            }

            // MatchOnlyTextFieldMapper class lives in the mapper-extras module. We use string equality
            // for the field type name to avoid adding a dependency to the module
            if (fieldType instanceof TextFieldMapper.TextFieldType || "match_only_text".equals(fieldType.typeName())) {
                var vs = textValueSource(ctx, fieldType);
                sources.add(new ValueSourceInfo(CoreValuesSourceType.KEYWORD, vs, elementType, ctx.getIndexReader()));
                continue;
            }

            IndexFieldData<?> fieldData;
            try {
                fieldData = ctx.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
            } catch (IllegalArgumentException e) {
                if (asUnsupportedSource) {
                    sources.add(
                        new ValueSourceInfo(
                            new UnsupportedValueSourceType(fieldType.typeName()),
                            new UnsupportedValueSource(null),
                            elementType,
                            ctx.getIndexReader()
                        )
                    );
                    continue;
                } else {
                    throw e;
                }
            }
            var fieldContext = new FieldContext(fieldName, fieldData, fieldType);
            var vsType = fieldData.getValuesSourceType();
            var vs = vsType.getField(fieldContext, null);

            if (asUnsupportedSource) {
                sources.add(
                    new ValueSourceInfo(
                        new UnsupportedValueSourceType(fieldType.typeName()),
                        new UnsupportedValueSource(vs),
                        elementType,
                        ctx.getIndexReader()
                    )
                );
            } else {
                sources.add(new ValueSourceInfo(vsType, vs, elementType, ctx.getIndexReader()));
            }
        }

        return sources;
    }

    private static TextValueSource textValueSource(SearchExecutionContext ctx, MappedFieldType fieldType) {
        if (fieldType.isStored()) {
            IndexFieldData<?> fieldData = new StoredFieldSortedBinaryIndexFieldData(
                fieldType.name(),
                CoreValuesSourceType.KEYWORD,
                TextDocValuesField::new
            ) {
                @Override
                protected BytesRef storedToBytesRef(Object stored) {
                    return new BytesRef((String) stored);
                }
            };
            return new TextValueSource(fieldData);
        }

        FieldDataContext fieldDataContext = new FieldDataContext(
            ctx.getFullyQualifiedIndex().getName(),
            () -> ctx.lookup().forkAndTrackFieldReferences(fieldType.name()),
            ctx::sourcePath,
            MappedFieldType.FielddataOperation.SEARCH
        );
        IndexFieldData<?> fieldData = new SourceValueFetcherSortedBinaryIndexFieldData.Builder(
            fieldType.name(),
            CoreValuesSourceType.KEYWORD,
            SourceValueFetcher.toString(fieldDataContext.sourcePathsLookup().apply(fieldType.name())),
            fieldDataContext.lookupSupplier().get(),
            TextDocValuesField::new
        ).build(null, null); // Neither cache nor breakerService are used by SourceValueFetcherSortedBinaryIndexFieldData builder
        return new TextValueSource(fieldData);
    }
}
