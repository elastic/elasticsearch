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
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.field.DocValuesScriptFieldSource;
import org.elasticsearch.script.field.ToScriptFieldSource;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;

public final class LongScriptFieldData extends IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final LongFieldScript.LeafFactory leafFactory;
        protected final ToScriptFieldSource<SortedNumericDocValues> toScriptFieldSource;

        public Builder(
            String name,
            LongFieldScript.LeafFactory leafFactory,
            ToScriptFieldSource<SortedNumericDocValues> toScriptFieldSource
        ) {
            this.name = name;
            this.leafFactory = leafFactory;
            this.toScriptFieldSource = toScriptFieldSource;
        }

        @Override
        public LongScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new LongScriptFieldData(name, leafFactory, toScriptFieldSource);
        }
    }

    private final String fieldName;
    private final LongFieldScript.LeafFactory leafFactory;
    protected final ToScriptFieldSource<SortedNumericDocValues> toScriptFieldSource;

    private LongScriptFieldData(
        String fieldName,
        LongFieldScript.LeafFactory leafFactory,
        ToScriptFieldSource<SortedNumericDocValues> toScriptFieldSource
    ) {
        this.fieldName = fieldName;
        this.leafFactory = leafFactory;
        this.toScriptFieldSource = toScriptFieldSource;
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
        return new LongScriptLeafFieldData(new LongScriptDocValues(leafFactory.newInstance(context)), toScriptFieldSource);
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
        protected final ToScriptFieldSource<SortedNumericDocValues> toScriptFieldSource;

        LongScriptLeafFieldData(LongScriptDocValues longScriptDocValues, ToScriptFieldSource<SortedNumericDocValues> toScriptFieldSource) {
            super(0);
            this.longScriptDocValues = longScriptDocValues;
            this.toScriptFieldSource = toScriptFieldSource;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return longScriptDocValues;
        }

        @Override
        public DocValuesScriptFieldSource getScriptFieldSource(String name) {
            return toScriptFieldSource.getScriptFieldSource(getLongValues(), name);
        }
    }
}
