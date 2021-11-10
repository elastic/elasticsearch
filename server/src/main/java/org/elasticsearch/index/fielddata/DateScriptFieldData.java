/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.index.fielddata.plain.LeafLongFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.field.DocValuesField;
import org.elasticsearch.script.field.ToScriptField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

public final class DateScriptFieldData extends IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final DateFieldScript.LeafFactory leafFactory;
        protected final ToScriptField<SortedNumericDocValues> toScriptField;

        public Builder(String name, DateFieldScript.LeafFactory leafFactory, ToScriptField<SortedNumericDocValues> toScriptField) {
            this.name = name;
            this.leafFactory = leafFactory;
            this.toScriptField = toScriptField;
        }

        @Override
        public DateScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new DateScriptFieldData(name, leafFactory, toScriptField);
        }
    }

    private final String fieldName;
    private final DateFieldScript.LeafFactory leafFactory;
    protected final ToScriptField<SortedNumericDocValues> toScriptField;

    private DateScriptFieldData(
        String fieldName,
        DateFieldScript.LeafFactory leafFactory,
        ToScriptField<SortedNumericDocValues> toScriptField
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
        return new DateScriptLeafFieldData(new LongScriptDocValues(leafFactory.newInstance(context)), toScriptField);
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
        protected final ToScriptField<SortedNumericDocValues> toScriptField;

        DateScriptLeafFieldData(LongScriptDocValues longScriptDocValues, ToScriptField<SortedNumericDocValues> toScriptField) {
            super(0);
            this.longScriptDocValues = longScriptDocValues;
            this.toScriptField = toScriptField;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return longScriptDocValues;
        }

        @Override
        public DocValuesField<?> getScriptField(String name) {
            return toScriptField.getScriptField(getLongValues(), name);
        }
    }
}
