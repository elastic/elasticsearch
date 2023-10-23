/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.StoredFieldSortedBinaryIndexFieldData;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class ValueSources {

    public static final String MATCH_ONLY_TEXT = "match_only_text";

    private ValueSources() {}

    public static List<ValueSourceInfo> sources(
        List<SearchContext> searchContexts,
        String fieldName,
        boolean asUnsupportedSource,
        ElementType elementType
    ) {
        List<ValueSourceInfo> sources = new ArrayList<>(searchContexts.size());

        for (SearchContext searchContext : searchContexts) {
            // TODO: remove this workaround
            // Create a separate SearchExecutionContext for each ValuesReader, as it seems that
            // the synthetic source doesn't work properly with inter-segment or intra-segment parallelism.
            ShardSearchRequest shardRequest = searchContext.request();
            SearchExecutionContext ctx = searchContext.readerContext()
                .indexService()
                .newSearchExecutionContext(
                    shardRequest.shardId().id(),
                    shardRequest.shardRequestIndex(),
                    searchContext.searcher(),
                    shardRequest::nowInMillis,
                    shardRequest.getClusterAlias(),
                    shardRequest.getRuntimeMappings()
                );
            var fieldType = ctx.getFieldType(fieldName);
            if (fieldType == null) {
                sources.add(new ValueSourceInfo(new NullValueSourceType(), new NullValueSource(), elementType, ctx.getIndexReader()));
                continue; // the field does not exist in this context
            }
            if (asUnsupportedSource) {
                sources.add(
                    new ValueSourceInfo(
                        new UnsupportedValueSourceType(fieldType.typeName()),
                        new UnsupportedValueSource(null),
                        elementType,
                        ctx.getIndexReader()
                    )
                );
                HeaderWarning.addWarning("Field [{}] cannot be retrieved, it is unsupported or not indexed; returning null", fieldName);
                continue;
            }

            if (fieldType.hasDocValues() == false) {
                // MatchOnlyTextFieldMapper class lives in the mapper-extras module. We use string equality
                // for the field type name to avoid adding a dependency to the module
                if (fieldType instanceof KeywordFieldMapper.KeywordFieldType
                    || fieldType instanceof TextFieldMapper.TextFieldType tft && (tft.isSyntheticSource() == false || tft.isStored())
                    || MATCH_ONLY_TEXT.equals(fieldType.typeName())) {
                    ValuesSource vs = textValueSource(ctx, fieldType);
                    sources.add(new ValueSourceInfo(CoreValuesSourceType.KEYWORD, vs, elementType, ctx.getIndexReader()));
                    continue;
                }

                if (IdFieldMapper.NAME.equals(fieldType.name())) {
                    ValuesSource vs = new IdValueSource(new IdFieldIndexFieldData(CoreValuesSourceType.KEYWORD));
                    sources.add(new ValueSourceInfo(CoreValuesSourceType.KEYWORD, vs, elementType, ctx.getIndexReader()));
                    continue;
                }
            }

            IndexFieldData<?> fieldData;
            try {
                fieldData = ctx.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
            } catch (IllegalArgumentException e) {
                sources.add(unsupportedValueSource(elementType, ctx, fieldType, e));
                HeaderWarning.addWarning("Field [{}] cannot be retrieved, it is unsupported or not indexed; returning null", fieldName);
                continue;
            }
            var fieldContext = new FieldContext(fieldName, fieldData, fieldType);
            var vsType = fieldData.getValuesSourceType();
            var vs = vsType.getField(fieldContext, null);
            sources.add(new ValueSourceInfo(vsType, vs, elementType, ctx.getIndexReader()));
        }

        return sources;
    }

    private static ValueSourceInfo unsupportedValueSource(
        ElementType elementType,
        SearchExecutionContext ctx,
        MappedFieldType fieldType,
        IllegalArgumentException e
    ) {
        return switch (elementType) {
            case BYTES_REF -> new ValueSourceInfo(
                new UnsupportedValueSourceType(fieldType.typeName()),
                new UnsupportedValueSource(null),
                elementType,
                ctx.getIndexReader()
            );
            case LONG, INT -> new ValueSourceInfo(
                CoreValuesSourceType.NUMERIC,
                ValuesSource.Numeric.EMPTY,
                elementType,
                ctx.getIndexReader()
            );
            case BOOLEAN -> new ValueSourceInfo(
                CoreValuesSourceType.BOOLEAN,
                ValuesSource.Numeric.EMPTY,
                elementType,
                ctx.getIndexReader()
            );
            case DOUBLE -> new ValueSourceInfo(CoreValuesSourceType.NUMERIC, new ValuesSource.Numeric() {
                @Override
                public boolean isFloatingPoint() {
                    return true;
                }

                @Override
                public SortedNumericDocValues longValues(LeafReaderContext context) {
                    return DocValues.emptySortedNumeric();
                }

                @Override
                public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                    return org.elasticsearch.index.fielddata.FieldData.emptySortedNumericDoubles();
                }

                @Override
                public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                    return org.elasticsearch.index.fielddata.FieldData.emptySortedBinary();
                }
            }, elementType, ctx.getIndexReader());
            default -> throw e;
        };
    }

    private static TextValueSource textValueSource(SearchExecutionContext ctx, MappedFieldType fieldType) {
        if (fieldType.isStored()) {
            IndexFieldData<?> fieldData = new StoredFieldSortedBinaryIndexFieldData(
                fieldType.name(),
                CoreValuesSourceType.KEYWORD,
                TextValueSource.TextDocValuesFieldWrapper::new
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
            TextValueSource.TextDocValuesFieldWrapper::new
        ).build(null, null); // Neither cache nor breakerService are used by SourceValueFetcherSortedBinaryIndexFieldData builder
        return new TextValueSource(fieldData);
    }
}
