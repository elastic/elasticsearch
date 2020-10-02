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
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.runtimefields.mapper.DateFieldScript;

public final class DateScriptFieldData extends IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final DateFieldScript.LeafFactory leafFactory;

        public Builder(String name, DateFieldScript.LeafFactory leafFactory) {
            this.name = name;
            this.leafFactory = leafFactory;
        }

        @Override
        public DateScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new DateScriptFieldData(name, leafFactory);
        }
    }

    private final String fieldName;
    private final DateFieldScript.LeafFactory leafFactory;

    private DateScriptFieldData(String fieldName, DateFieldScript.LeafFactory leafFactory) {
        this.fieldName = fieldName;
        this.leafFactory = leafFactory;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.DATE;
    }

    @Override
    public DateScriptLeafFieldData load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public DateScriptLeafFieldData loadDirect(LeafReaderContext context) {
        return new DateScriptLeafFieldData(new LongScriptDocValues(leafFactory.newInstance(context)));
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.DATE;
    }

    @Override
    protected boolean sortRequiresCustomComparator() {
        return true;
    }

    public static class DateScriptLeafFieldData extends LeafLongFieldData {
        private final LongScriptDocValues longScriptDocValues;

        DateScriptLeafFieldData(LongScriptDocValues longScriptDocValues) {
            super(0, NumericType.DATE);
            this.longScriptDocValues = longScriptDocValues;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return longScriptDocValues;
        }
    }
}
