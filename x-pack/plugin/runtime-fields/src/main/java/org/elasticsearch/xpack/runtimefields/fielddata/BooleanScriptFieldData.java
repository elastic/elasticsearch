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

public final class BooleanScriptFieldData extends IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final BooleanFieldScript.LeafFactory leafFactory;

        public Builder(String name, BooleanFieldScript.LeafFactory leafFactory) {
            this.name = name;
            this.leafFactory = leafFactory;
        }

        @Override
        public BooleanScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
            return new BooleanScriptFieldData(name, leafFactory);
        }
    }

    private final String fieldName;
    private final BooleanFieldScript.LeafFactory leafFactory;

    private BooleanScriptFieldData(String fieldName, BooleanFieldScript.LeafFactory leafFactory) {
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
    public BooleanScriptLeafFieldData load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public BooleanScriptLeafFieldData loadDirect(LeafReaderContext context) {
        return new BooleanScriptLeafFieldData(new BooleanScriptDocValues(leafFactory.newInstance(context)));
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.BOOLEAN;
    }

    @Override
    protected boolean sortRequiresCustomComparator() {
        return true;
    }

    public static class BooleanScriptLeafFieldData extends LeafLongFieldData {
        private final BooleanScriptDocValues booleanScriptDocValues;

        BooleanScriptLeafFieldData(BooleanScriptDocValues booleanScriptDocValues) {
            super(0, NumericType.BOOLEAN);
            this.booleanScriptDocValues = booleanScriptDocValues;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return booleanScriptDocValues;
        }

        @Override
        public void close() {}
    }
}
