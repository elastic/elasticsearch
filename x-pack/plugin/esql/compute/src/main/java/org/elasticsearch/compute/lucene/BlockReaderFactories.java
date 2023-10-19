/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.index.mapper.BlockDocValuesReader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class BlockReaderFactories {
    private BlockReaderFactories() {}

    public static List<BlockDocValuesReader.Factory> factories(
        List<SearchContext> searchContexts,
        String fieldName,
        boolean asUnsupportedSource
    ) {
        List<BlockDocValuesReader.Factory> factories = new ArrayList<>(searchContexts.size());

        for (SearchContext searchContext : searchContexts) {
            SearchExecutionContext ctx = searchContext.getSearchExecutionContext();
            if (asUnsupportedSource) {
                factories.add(loaderToFactory(ctx.getIndexReader(), BlockDocValuesReader.nulls()));
                continue;
            }
            MappedFieldType fieldType = ctx.getFieldType(fieldName);
            if (fieldType == null) {
                // the field does not exist in this context
                factories.add(loaderToFactory(ctx.getIndexReader(), BlockDocValuesReader.nulls()));
                continue;
            }
            BlockLoader loader = fieldType.blockLoader(
                ft -> ctx.getForField(ft, MappedFieldType.FielddataOperation.SEARCH),
                ctx::sourcePath
            );
            if (loader == null) {
                HeaderWarning.addWarning("Field [{}] cannot be retrieved, it is unsupported or not indexed; returning null", fieldName);
                factories.add(loaderToFactory(ctx.getIndexReader(), BlockDocValuesReader.nulls()));
                continue;
            }
            factories.add(loaderToFactory(ctx.getIndexReader(), loader));
        }

        return factories;
    }

    public static BlockDocValuesReader.Factory loaderToFactory(IndexReader reader, BlockLoader loader) {
        return new BlockDocValuesReader.Factory() {
            @Override
            public BlockDocValuesReader build(int segment) throws IOException {
                return loader.reader(reader.leaves().get(segment));
            }

            @Override
            public boolean supportsOrdinals() {
                return loader.supportsOrdinals();
            }

            @Override
            public SortedSetDocValues ordinals(int segment) throws IOException {
                return loader.ordinals(reader.leaves().get(segment));
            }
        };
    }
}
