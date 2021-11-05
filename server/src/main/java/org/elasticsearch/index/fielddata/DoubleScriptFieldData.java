/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.index.fielddata.plain.LeafDoubleFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.field.DocValuesField;
import org.elasticsearch.script.field.ToScriptField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

public final class DoubleScriptFieldData extends IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final DoubleFieldScript.LeafFactory leafFactory;
        protected final ToScriptField<SortedNumericDoubleValues> toScriptField;

        public Builder(String name, DoubleFieldScript.LeafFactory leafFactory, ToScriptField<SortedNumericDoubleValues> toScriptField) {
            this.name = name;
            this.leafFactory = leafFactory;
            this.toScriptField = toScriptField;
        }

        @Override
        public DoubleScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new DoubleScriptFieldData(name, leafFactory, toScriptField);
        }
    }

    private final String fieldName;
    DoubleFieldScript.LeafFactory leafFactory;
    protected final ToScriptField<SortedNumericDoubleValues> toScriptField;

    private DoubleScriptFieldData(
        String fieldName,
        DoubleFieldScript.LeafFactory leafFactory,
        ToScriptField<SortedNumericDoubleValues> toScriptField
    ) {
        this.fieldName = fieldName;
        this.leafFactory = leafFactory;
        this.toScriptField = toScriptField;
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
        return new DoubleScriptLeafFieldData(new DoubleScriptDocValues(leafFactory.newInstance(context)), toScriptField);
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
        protected final ToScriptField<SortedNumericDoubleValues> toScriptField;

        DoubleScriptLeafFieldData(DoubleScriptDocValues doubleScriptDocValues, ToScriptField<SortedNumericDoubleValues> toScriptField) {
            super(0);
            this.doubleScriptDocValues = doubleScriptDocValues;
            this.toScriptField = toScriptField;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            return doubleScriptDocValues;
        }

        @Override
        public void close() {}

        @Override
        public DocValuesField<?> getScriptField(String name) {
            return toScriptField.getScriptField(getDoubleValues(), name);
        }
    }
}
