/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Set;

public class DummyBlockLoaderContext implements MappedFieldType.BlockLoaderContext {
    private final String indexName;

    public DummyBlockLoaderContext(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public final String indexName() {
        return indexName;
    }

    @Override
    public IndexSettings indexSettings() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
        return MappedFieldType.FieldExtractPreference.NONE;
    }

    @Override
    public SearchLookup lookup() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> sourcePaths(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String parentField(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappingLookup mappingLookup() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
        return null;
    }

    @Override
    public ByteSizeValue ordinalsByteSize() {
        return DEFAULT_ORDINALS_BYTE_SIZE;
    }

    @Override
    public ByteSizeValue scriptByteSize() {
        return DEFAULT_SCRIPT_BYTE_SIZE;
    }

    public static class MapperServiceBlockLoaderContext extends DummyBlockLoaderContext {
        private final MapperService mapperService;

        public MapperServiceBlockLoaderContext(MapperService mapperService) {
            super(mapperService.index().getName());
            this.mapperService = mapperService;
        }

        @Override
        public IndexSettings indexSettings() {
            return mapperService.getIndexSettings();
        }

        @Override
        public SearchLookup lookup() {
            return new SearchLookup(mapperService.mappingLookup().fieldTypesLookup()::get, null, null);
        }

        @Override
        public Set<String> sourcePaths(String name) {
            return mapperService.mappingLookup().sourcePaths(name);
        }

        @Override
        public String parentField(String field) {
            return mapperService.mappingLookup().parentField(field);
        }

        @Override
        public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
            return (FieldNamesFieldMapper.FieldNamesFieldType) mapperService.fieldType(FieldNamesFieldMapper.NAME);
        }

        @Override
        public MappingLookup mappingLookup() {
            return mapperService.mappingLookup();
        }
    }
}
