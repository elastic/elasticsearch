/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SearchLookupAware;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.runtimefields.LongScriptFieldScript;

import java.io.IOException;

public final class ScriptLongFieldData extends IndexNumericFieldData implements SearchLookupAware {

    public static class Builder implements IndexFieldData.Builder {

        private final Script script;
        private final LongScriptFieldScript.Factory scriptFactory;

        public Builder(Script script, LongScriptFieldScript.Factory scriptFactory) {
            this.script = script;
            this.scriptFactory = scriptFactory;
        }

        @Override
        public ScriptLongFieldData build(
            IndexSettings indexSettings,
            MappedFieldType fieldType,
            IndexFieldDataCache cache,
            CircuitBreakerService breakerService,
            MapperService mapperService
        ) {
            return new ScriptLongFieldData(indexSettings.getIndex(), fieldType.name(), script, scriptFactory);
        }
    }

    private final Index index;
    private final String fieldName;
    private final Script script;
    private final LongScriptFieldScript.Factory scriptFactory;
    private final SetOnce<LongScriptFieldScript.LeafFactory> leafFactory = new SetOnce<>();

    private ScriptLongFieldData(Index index, String fieldName, Script script, LongScriptFieldScript.Factory scriptFactory) {
        this.index = index;
        this.fieldName = fieldName;
        this.script = script;
        this.scriptFactory = scriptFactory;
    }

    @Override
    public void setSearchLookup(SearchLookup searchLookup) {
        this.leafFactory.set(scriptFactory.newFactory(script.getParams(), searchLookup));
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
        return new ScriptLongLeafFieldData(new ScriptLongDocValues(leafFactory.get().newInstance(context)));
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.LONG;
    }

    @Override
    protected boolean sortRequiresCustomComparator() {
        return true;
    }

    @Override
    public void clear() {}

    public static class ScriptLongLeafFieldData implements LeafNumericFieldData {
        private final ScriptLongDocValues scriptBinaryDocValues;

        ScriptLongLeafFieldData(ScriptLongDocValues scriptBinaryDocValues) {
            this.scriptBinaryDocValues = scriptBinaryDocValues;
        }

        @Override
        public ScriptDocValues<?> getScriptValues() {
            return new ScriptDocValues.Longs(getLongValues());
        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
            return FieldData.toString(scriptBinaryDocValues);
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            return FieldData.castToDouble(getLongValues());
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return scriptBinaryDocValues;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public void close() {

        }
    }
}
