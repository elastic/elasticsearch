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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsIndexFieldData;
import org.elasticsearch.index.fielddata.ordinals.OnDiskGlobalOrdinalMap;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

public class SortedSetOrdinalsIndexFieldDataWithGlobalsOnDisk extends SortedSetOrdinalsIndexFieldData {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction;
        private final ValuesSourceType valuesSourceType;

        public Builder(String name, ValuesSourceType valuesSourceType) {
            this(name, AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION, valuesSourceType);
        }

        public Builder(String name, Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction, ValuesSourceType valuesSourceType) {
            this.name = name;
            this.scriptFunction = scriptFunction;
            this.valuesSourceType = valuesSourceType;
        }

        @Override
        public SortedSetOrdinalsIndexFieldDataWithGlobalsOnDisk build(
            IndexFieldDataCache cache,
            CircuitBreakerService breakerService
        ) {
            return new SortedSetOrdinalsIndexFieldDataWithGlobalsOnDisk(cache, name, valuesSourceType, breakerService, scriptFunction);
        }
    }

    public SortedSetOrdinalsIndexFieldDataWithGlobalsOnDisk(
        IndexFieldDataCache cache,
        String fieldName,
        ValuesSourceType valuesSourceType,
        CircuitBreakerService breakerService,
        Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction
    ) {
        super(cache, fieldName, valuesSourceType, breakerService, scriptFunction);
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        IndexOrdinalsFieldData loaded = super.loadGlobal(indexReader);
        if (loaded == this) {
            // If there is only a single segment we'll get ourselves back.
            return loaded;
        }
        if (loaded instanceof GlobalOrdinalsIndexFieldData.Consumer) {
            // loadGlobal probably loaded the "empty" implementation
            return loaded;
        }
        try {
            return ((OnDiskGlobalOrdinalMap) loaded).fork();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception {
        return new OnDiskGlobalOrdinalMap(indexReader.directory(), this, indexReader, breakerService);
    }

    @Override
    public LongUnaryOperator getOrdinalMapping(LeafReaderContext context) {
        /*
         * This will only be called if this is a single segment index. If this
         * *isn't* a single segment index then the caller should have ended up
         * getting back a forked OnDiskGlobalOrdinalMap. 
         */
        assert context.ordInParent == 0; 
        return LongUnaryOperator.identity();
    }

    @Override
    public boolean supportsGlobalOrdinalsMapping() {
        return true;
    }
}
