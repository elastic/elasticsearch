/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SearchLookupAware;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.io.IOException;

public final class ScriptBinaryFieldData implements IndexFieldData<ScriptBinaryFieldData.ScriptBinaryLeafFieldData>, SearchLookupAware {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final Script script;
        private final StringScriptFieldScript.Factory scriptFactory;

        public Builder(String name, Script script, StringScriptFieldScript.Factory scriptFactory) {
            this.name = name;
            this.script = script;
            this.scriptFactory = scriptFactory;
        }

        @Override
        public ScriptBinaryFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
            return new ScriptBinaryFieldData(name, script, scriptFactory);
        }
    }

    private final String fieldName;
    private final Script script;
    private final StringScriptFieldScript.Factory scriptFactory;
    private final SetOnce<StringScriptFieldScript.LeafFactory> leafFactory = new SetOnce<>();

    private ScriptBinaryFieldData(String fieldName, Script script, StringScriptFieldScript.Factory scriptFactory) {
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
        return CoreValuesSourceType.BYTES;
    }

    @Override
    public ScriptBinaryLeafFieldData load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public ScriptBinaryLeafFieldData loadDirect(LeafReaderContext context) throws IOException {
        return new ScriptBinaryLeafFieldData(new ScriptBinaryDocValues(leafFactory.get().newInstance(context)));
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        final XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        return new SortField(getFieldName(), source, reverse);
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("only supported on numeric fields");
    }

    @Override
    public void clear() {

    }

    public static class ScriptBinaryLeafFieldData implements LeafFieldData {
        private final ScriptBinaryDocValues scriptBinaryDocValues;

        ScriptBinaryLeafFieldData(ScriptBinaryDocValues scriptBinaryDocValues) {
            this.scriptBinaryDocValues = scriptBinaryDocValues;
        }

        @Override
        public ScriptDocValues<?> getScriptValues() {
            return new ScriptDocValues.Strings(getBytesValues());
        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
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
