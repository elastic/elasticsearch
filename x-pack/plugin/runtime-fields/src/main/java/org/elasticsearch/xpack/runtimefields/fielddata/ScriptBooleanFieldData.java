/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.LeafLongFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.runtimefields.mapper.BooleanFieldScript;

import java.io.IOException;

public final class ScriptBooleanFieldData extends IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final BooleanFieldScript.LeafFactory leafFactory;

        public Builder(String name, BooleanFieldScript.LeafFactory leafFactory) {
            this.name = name;
            this.leafFactory = leafFactory;
        }

        @Override
        public ScriptBooleanFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
            return new ScriptBooleanFieldData(name, leafFactory);
        }
    }

    private final String fieldName;
    private final BooleanFieldScript.LeafFactory leafFactory;

    private ScriptBooleanFieldData(String fieldName, BooleanFieldScript.LeafFactory leafFactory) {
        this.fieldName = fieldName;
        this.leafFactory = leafFactory;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.BOOLEAN;
    }

    @Override
    public ScriptBooleanLeafFieldData load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public ScriptBooleanLeafFieldData loadDirect(LeafReaderContext context) throws IOException {
        return new ScriptBooleanLeafFieldData(new ScriptBooleanDocValues(leafFactory.newInstance(context)));
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.BOOLEAN;
    }

    @Override
    protected boolean sortRequiresCustomComparator() {
        return true;
    }

    public static class ScriptBooleanLeafFieldData extends LeafLongFieldData {
        private final ScriptBooleanDocValues scriptBooleanDocValues;

        ScriptBooleanLeafFieldData(ScriptBooleanDocValues scriptBooleanDocValues) {
            super(0, NumericType.BOOLEAN);
            this.scriptBooleanDocValues = scriptBooleanDocValues;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return scriptBooleanDocValues;
        }

        @Override
        public void close() {}
    }
}
