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
import org.elasticsearch.xpack.runtimefields.LongScriptFieldScript;

import java.io.IOException;

public final class ScriptLongFieldData extends IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final LongScriptFieldScript.LeafFactory leafFactory;

        public Builder(String name, LongScriptFieldScript.LeafFactory leafFactory) {
            this.name = name;
            this.leafFactory = leafFactory;
        }

        @Override
        public ScriptLongFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
            return new ScriptLongFieldData(name, leafFactory);
        }
    }

    private final String fieldName;
    private final LongScriptFieldScript.LeafFactory leafFactory;

    private ScriptLongFieldData(String fieldName, LongScriptFieldScript.LeafFactory leafFactory) {
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
    public ScriptLongLeafFieldData load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public ScriptLongLeafFieldData loadDirect(LeafReaderContext context) throws IOException {
        return new ScriptLongLeafFieldData(new ScriptLongDocValues(leafFactory.newInstance(context)));
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.LONG;
    }

    @Override
    protected boolean sortRequiresCustomComparator() {
        return true;
    }

    public static class ScriptLongLeafFieldData extends LeafLongFieldData {
        private final ScriptLongDocValues scriptLongDocValues;

        ScriptLongLeafFieldData(ScriptLongDocValues scriptLongDocValues) {
            super(0, NumericType.LONG);
            this.scriptLongDocValues = scriptLongDocValues;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return scriptLongDocValues;
        }
    }
}
