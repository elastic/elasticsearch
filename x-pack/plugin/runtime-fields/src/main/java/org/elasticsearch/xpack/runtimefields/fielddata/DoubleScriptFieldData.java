/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.plain.LeafDoubleFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.runtimefields.mapper.DoubleFieldScript;

public final class DoubleScriptFieldData extends IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final DoubleFieldScript.LeafFactory leafFactory;

        public Builder(String name, DoubleFieldScript.LeafFactory leafFactory) {
            this.name = name;
            this.leafFactory = leafFactory;
        }

        @Override
        public DoubleScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new DoubleScriptFieldData(name, leafFactory);
        }
    }

    private final String fieldName;
    DoubleFieldScript.LeafFactory leafFactory;

    private DoubleScriptFieldData(String fieldName, DoubleFieldScript.LeafFactory leafFactory) {
        this.fieldName = fieldName;
        this.leafFactory = leafFactory;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    public DoubleScriptLeafFieldData load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public DoubleScriptLeafFieldData loadDirect(LeafReaderContext context) {
        return new DoubleScriptLeafFieldData(new DoubleScriptDocValues(leafFactory.newInstance(context)));
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.DOUBLE;
    }

    @Override
    protected boolean sortRequiresCustomComparator() {
        return true;
    }

    public static class DoubleScriptLeafFieldData extends LeafDoubleFieldData {
        private final DoubleScriptDocValues doubleScriptDocValues;

        DoubleScriptLeafFieldData(DoubleScriptDocValues doubleScriptDocValues) {
            super(0);
            this.doubleScriptDocValues = doubleScriptDocValues;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            return doubleScriptDocValues;
        }

        @Override
        public void close() {}
    }
}
