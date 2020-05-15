/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.function.Function;

public class SortedSetOrdinalsIndexFieldData implements IndexOrdinalsFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction;

        public Builder() {
            this(AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION);
        }

        public Builder(Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction) {
            this.scriptFunction = scriptFunction;
        }

        @Override
        public SortedSetOrdinalsIndexFieldData build(
            IndexSettings indexSettings,
            MappedFieldType fieldType,
            IndexFieldDataCache cache,
            CircuitBreakerService breakerService,
            MapperService mapperService
        ) {
            final String fieldName = fieldType.name();
            return new SortedSetOrdinalsIndexFieldData(indexSettings, cache, fieldName, breakerService, scriptFunction);
        }
    }

    protected final Index index;
    protected final String fieldName;
    private final IndexSettings indexSettings;
    private final IndexFieldDataCache cache;
    private final CircuitBreakerService breakerService;
    private final Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction;
    private static final Logger logger = LogManager.getLogger(SortedSetOrdinalsIndexFieldData.class);

    public SortedSetOrdinalsIndexFieldData(
        IndexSettings indexSettings,
        IndexFieldDataCache cache,
        String fieldName,
        CircuitBreakerService breakerService,
        Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction
    ) {
        this.index = indexSettings.getIndex();
        this.fieldName = fieldName;
        this.indexSettings = indexSettings;
        this.cache = cache;
        this.breakerService = breakerService;
        this.scriptFunction = scriptFunction;
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    @Override
    public final void clear() {
        // can't do
    }

    @Override
    public final Index index() {
        return index;
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        /**
         * Check if we can use a simple {@link SortedSetSortField} compatible with index sorting and
         * returns a custom sort field otherwise.
         */
        if (nested != null ||
                (sortMode != MultiValueMode.MAX && sortMode != MultiValueMode.MIN) ||
                (source.sortMissingLast(missingValue) == false && source.sortMissingFirst(missingValue) == false)) {
            return new SortField(getFieldName(), source, reverse);
        }
        SortField sortField = new SortedSetSortField(fieldName, reverse,
            sortMode == MultiValueMode.MAX ? SortedSetSelector.Type.MAX : SortedSetSelector.Type.MIN);
        sortField.setMissingValue(source.sortMissingLast(missingValue) ^ reverse ?
            SortedSetSortField.STRING_LAST : SortedSetSortField.STRING_FIRST);
        return sortField;
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, Nested nested,
            SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
        throw new IllegalArgumentException("only supported on numeric fields");
    }

    @Override
    public LeafOrdinalsFieldData load(LeafReaderContext context) {
        return new SortedSetBytesLeafFieldData(context.reader(), fieldName, scriptFunction);
    }

    @Override
    public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        IndexOrdinalsFieldData fieldData = loadGlobalInternal(indexReader);
        if (fieldData instanceof GlobalOrdinalsIndexFieldData) {
            // we create a new instance of the cached value for each consumer in order
            // to avoid creating new TermsEnums for each segment in the cached instance
            return ((GlobalOrdinalsIndexFieldData) fieldData).newConsumer(indexReader);
        } else {
            return fieldData;
        }
    }

    private IndexOrdinalsFieldData loadGlobalInternal(DirectoryReader indexReader) {
        if (indexReader.leaves().size() <= 1) {
            // ordinals are already global
            return this;
        }
        boolean fieldFound = false;
        for (LeafReaderContext context : indexReader.leaves()) {
            if (context.reader().getFieldInfos().fieldInfo(getFieldName()) != null) {
                fieldFound = true;
                break;
            }
        }
        if (fieldFound == false) {
            // Some directory readers may be wrapped and report different set of fields and use the same cache key.
            // If a field can't be found then it doesn't mean it isn't there,
            // so if a field doesn't exist then we don't cache it and just return an empty field data instance.
            // The next time the field is found, we do cache.
            try {
                return GlobalOrdinalsBuilder.buildEmpty(indexSettings, indexReader, this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return cache.load(indexReader, this);
        } catch (Exception e) {
            if (e instanceof ElasticsearchException) {
                throw (ElasticsearchException) e;
            } else {
                throw new ElasticsearchException(e);
            }
        }
    }

    @Override
    public IndexOrdinalsFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception {
        return GlobalOrdinalsBuilder.build(indexReader, this, indexSettings, breakerService, logger, scriptFunction);
    }

    @Override
    public OrdinalMap getOrdinalMap() {
        return null;
    }

    @Override
    public boolean supportsGlobalOrdinalsMapping() {
        return true;
    }
}
