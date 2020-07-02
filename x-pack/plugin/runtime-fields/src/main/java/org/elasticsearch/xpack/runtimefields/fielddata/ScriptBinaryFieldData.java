/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SearchLookupAware;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ScriptBinaryFieldData extends AbstractIndexComponent
    implements
        IndexFieldData<ScriptBinaryFieldData.ScriptBinaryLeafFieldData>,
        SearchLookupAware {

    public static class Builder implements IndexFieldData.Builder {

        private final StringScriptFieldScript.Factory scriptFactory;

        public Builder(StringScriptFieldScript.Factory scriptFactory) {
            this.scriptFactory = scriptFactory;
        }

        @Override
        public IndexFieldData<?> build(
            IndexSettings indexSettings,
            MappedFieldType fieldType,
            IndexFieldDataCache cache,
            CircuitBreakerService breakerService,
            MapperService mapperService
        ) {
            return new ScriptBinaryFieldData(indexSettings, fieldType.name(), scriptFactory);
        }
    }

    private final String fieldName;
    private final StringScriptFieldScript.Factory scriptFactory;
    private final SetOnce<StringScriptFieldScript.LeafFactory> leafFactory = new SetOnce<>();

    private ScriptBinaryFieldData(IndexSettings indexSettings, String fieldName, StringScriptFieldScript.Factory scriptFactory) {
        super(indexSettings);
        this.fieldName = fieldName;
        this.scriptFactory = scriptFactory;
    }

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
        return CoreValuesSourceType.BYTES;
    }

    @Override
    public ScriptBinaryLeafFieldData load(LeafReaderContext context) {
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
    public ScriptBinaryLeafFieldData loadDirect(LeafReaderContext context) throws IOException {
        ScriptBinaryResult scriptBinaryResult = new ScriptBinaryResult();
        return new ScriptBinaryLeafFieldData(
            new ScriptBinaryDocValues(leafFactory.get().newInstance(context, scriptBinaryResult::accept), scriptBinaryResult)
        );
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

    static class ScriptBinaryLeafFieldData implements LeafFieldData {
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
