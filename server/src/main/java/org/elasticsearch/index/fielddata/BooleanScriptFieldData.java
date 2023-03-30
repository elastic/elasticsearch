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
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

public final class BooleanScriptFieldData extends IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final BooleanFieldScript.LeafFactory leafFactory;
        protected final ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory;

        public Builder(
            String name,
            BooleanFieldScript.LeafFactory leafFactory,
            ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory
        ) {
            this.name = name;
            this.leafFactory = leafFactory;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public BooleanScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new BooleanScriptFieldData(name, leafFactory, toScriptFieldFactory);
        }
    }

    private final String fieldName;
    private final BooleanFieldScript.LeafFactory leafFactory;
    protected final ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory;

    private BooleanScriptFieldData(
        String fieldName,
        BooleanFieldScript.LeafFactory leafFactory,
        ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory
    ) {
        this.fieldName = fieldName;
        this.leafFactory = leafFactory;
        this.toScriptFieldFactory = toScriptFieldFactory;
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
        return new BooleanScriptLeafFieldData(new BooleanScriptDocValues(leafFactory.newInstance(context)), toScriptFieldFactory);
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
        protected final ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory;

        BooleanScriptLeafFieldData(
            BooleanScriptDocValues booleanScriptDocValues,
            ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory
        ) {
            super(0);
            this.booleanScriptDocValues = booleanScriptDocValues;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return booleanScriptDocValues;
        }

        @Override
        public void close() {}

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(getLongValues(), name);
        }
    }
}
