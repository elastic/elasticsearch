/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.runtimefields.LongScriptFieldScript;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ScriptLongFieldData extends IndexNumericFieldData implements SearchLookupAware {

    public static class Builder implements IndexFieldData.Builder {

        private final LongScriptFieldScript.Factory scriptFactory;

        public Builder(LongScriptFieldScript.Factory scriptFactory) {
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
            return new ScriptLongFieldData(indexSettings.getIndex(), fieldType.name(), scriptFactory);
        }
    }

    private final Index index;
    private final String fieldName;
    private final LongScriptFieldScript.Factory scriptFactory;
    private final SetOnce<LongScriptFieldScript.LeafFactory> leafFactory = new SetOnce<>();

    private ScriptLongFieldData(Index index, String fieldName, LongScriptFieldScript.Factory scriptFactory) {
        this.index = index;
        this.fieldName = fieldName;
        this.scriptFactory = scriptFactory;
    }

    @Override
    public void setSearchLookup(SearchLookup searchLookup) {
        // TODO wire the params from the mappings definition, we don't parse them yet
        this.leafFactory.set(scriptFactory.newFactory(Collections.emptyMap(), searchLookup));
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
            if (e instanceof ElasticsearchException) {
                throw (ElasticsearchException) e;
            } else {
                throw new ElasticsearchException(e);
            }
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

    @Override
    public Index index() {
        return index;
    }

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

    static class ScriptBinaryResult {
        private final List<String> result = new ArrayList<>();

        void accept(String value) {
            this.result.add(value);
        }

        List<String> getResult() {
            return result;
        }
    }
}
