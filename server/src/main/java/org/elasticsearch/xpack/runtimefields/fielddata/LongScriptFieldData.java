/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.LeafLongFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.runtimefields.mapper.LongFieldScript;

import java.io.IOException;

public final class LongScriptFieldData extends IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final LongFieldScript.LeafFactory leafFactory;

        public Builder(String name, LongFieldScript.LeafFactory leafFactory) {
            this.name = name;
            this.leafFactory = leafFactory;
        }

        @Override
        public LongScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new LongScriptFieldData(name, leafFactory);
        }
    }

    private final String fieldName;
    private final LongFieldScript.LeafFactory leafFactory;

    private LongScriptFieldData(String fieldName, LongFieldScript.LeafFactory leafFactory) {
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
    public LongScriptLeafFieldData load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public LongScriptLeafFieldData loadDirect(LeafReaderContext context) throws IOException {
        return new LongScriptLeafFieldData(new LongScriptDocValues(leafFactory.newInstance(context)));
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.LONG;
    }

    @Override
    protected boolean sortRequiresCustomComparator() {
        return true;
    }

    public static class LongScriptLeafFieldData extends LeafLongFieldData {
        private final LongScriptDocValues longScriptDocValues;

        LongScriptLeafFieldData(LongScriptDocValues longScriptDocValues) {
            super(0, NumericType.LONG);
            this.longScriptDocValues = longScriptDocValues;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return longScriptDocValues;
        }
    }
}
