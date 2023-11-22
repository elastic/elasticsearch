/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

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
    public static List<BlockLoader> loaders(List<SearchContext> searchContexts, String fieldName, boolean asUnsupportedSource) {
        List<BlockLoader> loaders = new ArrayList<>(searchContexts.size());

        for (SearchContext searchContext : searchContexts) {
            SearchExecutionContext ctx = searchContext.getSearchExecutionContext();
            if (asUnsupportedSource) {
                loaders.add(BlockLoader.CONSTANT_NULLS);
                continue;
            }
            MappedFieldType fieldType = ctx.getFieldType(fieldName);
            if (fieldType == null) {
                // the field does not exist in this context
                loaders.add(BlockLoader.CONSTANT_NULLS);
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
                loaders.add(BlockLoader.CONSTANT_NULLS);
                continue;
            }
            loaders.add(loader);
        }

        return loaders;
    }
}
