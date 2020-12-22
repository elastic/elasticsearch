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
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.OnDiskOrdinalMap;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.util.function.Function;

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
        return ((OnDiskOrdinalMap) super.loadGlobal(indexReader)).fork();
    }

    @Override
    public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception {
        return new OnDiskOrdinalMap(indexReader.directory(), this, indexReader, breakerService);
    }

    @Override
    public boolean supportsGlobalOrdinalsMapping() {
        return false;
    }
}
