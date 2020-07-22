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
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SearchLookupAware;
import org.elasticsearch.index.fielddata.plain.LeafLongFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.runtimefields.DateScriptFieldScript;

import java.io.IOException;

public final class ScriptDateFieldData extends IndexNumericFieldData implements SearchLookupAware {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final Script script;
        private final DateScriptFieldScript.Factory scriptFactory;

        public Builder(String name, Script script, DateScriptFieldScript.Factory scriptFactory) {
            this.name = name;
            this.script = script;
            this.scriptFactory = scriptFactory;
        }

        @Override
        public ScriptDateFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
            return new ScriptDateFieldData(name, script, scriptFactory);
        }
    }

    private final String fieldName;
    private final Script script;
    private final DateScriptFieldScript.Factory scriptFactory;
    private final SetOnce<DateScriptFieldScript.LeafFactory> leafFactory = new SetOnce<>();

    private ScriptDateFieldData(String fieldName, Script script, DateScriptFieldScript.Factory scriptFactory) {
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
        return CoreValuesSourceType.DATE;
    }

    @Override
    public ScriptDateLeafFieldData load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public ScriptDateLeafFieldData loadDirect(LeafReaderContext context) throws IOException {
        return new ScriptDateLeafFieldData(new ScriptLongDocValues(leafFactory.get().newInstance(context)));
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
    public void clear() {}

    public static class ScriptDateLeafFieldData extends LeafLongFieldData {
        private final ScriptLongDocValues scriptLongDocValues;

        ScriptDateLeafFieldData(ScriptLongDocValues scriptLongDocValues) {
            super(0, NumericType.DATE);
            this.scriptLongDocValues = scriptLongDocValues;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return scriptLongDocValues;
        }
    }
}
