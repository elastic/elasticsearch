/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Set;

/**
 * {@link MappedFieldType.BlockLoaderContext} implementation for ESQL.
 */
class EsqlBlockLoaderContext implements MappedFieldType.BlockLoaderContext {
    private final SearchExecutionContext ctx;
    private final MappedFieldType.FieldExtractPreference fieldExtractPreference;
    private final BlockLoaderFunctionConfig blockLoaderFunctionConfig;
    private final ByteSizeValue blockLoaderSizeOrdinals;
    private final ByteSizeValue blockLoaderSizeScript;

    EsqlBlockLoaderContext(
        SearchExecutionContext ctx,
        MappedFieldType.FieldExtractPreference fieldExtractPreference,
        BlockLoaderFunctionConfig blockLoaderFunctionConfig,
        ByteSizeValue blockLoaderSizeOrdinals,
        ByteSizeValue blockLoaderSizeScript
    ) {
        this.ctx = ctx;
        this.fieldExtractPreference = fieldExtractPreference;
        this.blockLoaderFunctionConfig = blockLoaderFunctionConfig;
        this.blockLoaderSizeOrdinals = blockLoaderSizeOrdinals;
        this.blockLoaderSizeScript = blockLoaderSizeScript;
    }

    @Override
    public String indexName() {
        return ctx.getFullyQualifiedIndex().getName();
    }

    @Override
    public IndexSettings indexSettings() {
        return ctx.getIndexSettings();
    }

    @Override
    public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
        return fieldExtractPreference;
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

    @Override
    public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
        return blockLoaderFunctionConfig;
    }

    @Override
    public MappingLookup mappingLookup() {
        return ctx.getMappingLookup();
    }

    @Override
    public ByteSizeValue ordinalsByteSize() {
        return blockLoaderSizeOrdinals;
    }

    @Override
    public ByteSizeValue scriptByteSize() {
        return blockLoaderSizeScript;
    }
}
