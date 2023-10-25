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
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Resolves *how* ESQL loads field values.
 */
public final class BlockReaderFactories {
    private BlockReaderFactories() {}

    /**
     * Resolves *how* ESQL loads field values.
     * @param searchContexts a search context per search index we're loading
     *                       field from
     * @param fieldName the name of the field to load
     * @param asUnsupportedSource should the field be loaded as "unsupported"?
     *                            These will always have {@code null} values
     */
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
            BlockLoader loader = fieldType.blockLoader(new MappedFieldType.BlockLoaderContext() {
                @Override
                public String indexName() {
                    return ctx.getFullyQualifiedIndex().getName();
                }

                @Override
                public SearchLookup lookup() {
                    return ctx.lookup();
                }

                @Override
                public Set<String> sourcePaths(String name) {
                    return ctx.sourcePath(name);
                }
            });
            if (loader == null) {
                HeaderWarning.addWarning("Field [{}] cannot be retrieved, it is unsupported or not indexed; returning null", fieldName);
                factories.add(loaderToFactory(ctx.getIndexReader(), BlockDocValuesReader.nulls()));
                continue;
            }
            factories.add(loaderToFactory(ctx.getIndexReader(), loader));
        }

        return factories;
    }

    /**
     * Converts a {@link BlockLoader}, something defined in core elasticsearch at
     * the field level, into a {@link BlockDocValuesReader.Factory} which can be
     * used inside ESQL.
     */
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
