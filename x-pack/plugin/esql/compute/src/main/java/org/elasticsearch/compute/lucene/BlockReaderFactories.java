/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Set;

/**
 * Resolves *how* ESQL loads field values.
 */
public final class BlockReaderFactories {
    private BlockReaderFactories() {}

    /**
     * Resolves *how* ESQL loads field values.
     * @param ctx a search context for the index we're loading field from
     * @param fieldName the name of the field to load
     * @param asUnsupportedSource should the field be loaded as "unsupported"?
     *                            These will always have {@code null} values
     */
    public static BlockLoader loader(SearchExecutionContext ctx, String fieldName, boolean asUnsupportedSource) {
        if (asUnsupportedSource) {
            return BlockLoader.CONSTANT_NULLS;
        }
        MappedFieldType fieldType = ctx.getFieldType(fieldName);
        if (fieldType == null) {
            // the field does not exist in this context
            return BlockLoader.CONSTANT_NULLS;
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

            @Override
            public String parentField(String field) {
                return ctx.parentPath(field);
            }

            @Override
            public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
                return (FieldNamesFieldMapper.FieldNamesFieldType) ctx.lookup().fieldType(FieldNamesFieldMapper.NAME);
            }
        });
        if (loader == null) {
            HeaderWarning.addWarning("Field [{}] cannot be retrieved, it is unsupported or not indexed; returning null", fieldName);
            return BlockLoader.CONSTANT_NULLS;
        }

        return loader;
    }
}
