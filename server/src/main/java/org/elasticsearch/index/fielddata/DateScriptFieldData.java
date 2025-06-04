/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.fielddata.plain.LeafLongFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

public final class DateScriptFieldData extends IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final DateFieldScript.LeafFactory leafFactory;
        protected final ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory;

        public Builder(
            String name,
            DateFieldScript.LeafFactory leafFactory,
            ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory
        ) {
            this.name = name;
            this.leafFactory = leafFactory;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public DateScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new DateScriptFieldData(name, leafFactory, toScriptFieldFactory);
        }
    }

    private final String fieldName;
    private final DateFieldScript.LeafFactory leafFactory;
    protected final ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory;

    private DateScriptFieldData(
        String fieldName,
        DateFieldScript.LeafFactory leafFactory,
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
        return new DateScriptLeafFieldData(new LongScriptDocValues(leafFactory.newInstance(context)), toScriptFieldFactory);
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.DATE;
    }

    @Override
    protected boolean sortRequiresCustomComparator() {
        return true;
    }

    @Override
    protected boolean isIndexed() {
        return false;
    }

    public static class DateScriptLeafFieldData extends LeafLongFieldData {
        private final LongScriptDocValues longScriptDocValues;
        protected final ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory;

        DateScriptLeafFieldData(
            LongScriptDocValues longScriptDocValues,
            ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory
        ) {
            super(0);
            this.longScriptDocValues = longScriptDocValues;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return longScriptDocValues;
        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(getLongValues(), name);
        }
    }
}
