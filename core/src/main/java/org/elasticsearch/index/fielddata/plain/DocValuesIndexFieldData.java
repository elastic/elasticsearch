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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.util.set.Sets.newHashSet;

/** {@link IndexFieldData} impl based on Lucene's doc values. Caching is done on the Lucene side. */
public abstract class DocValuesIndexFieldData {

    protected final Index index;
    protected final String fieldName;

    public DocValuesIndexFieldData(Index index, String fieldName) {
        super();
        this.index = index;
        this.fieldName = fieldName;
    }

    public final String getFieldName() {
        return fieldName;
    }

    public final void clear() {
        // can't do
    }

    public final void clear(IndexReader reader) {
        // can't do
    }

    public final Index index() {
        return index;
    }

    public static class Builder implements IndexFieldData.Builder {
        private static final Set<String> BINARY_INDEX_FIELD_NAMES = unmodifiableSet(newHashSet(UidFieldMapper.NAME, IdFieldMapper.NAME));

        private NumericType numericType;
        private Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction = AbstractAtomicOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION;

        public Builder numericType(NumericType type) {
            this.numericType = type;
            return this;
        }

        public Builder scriptFunction(Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction) {
            this.scriptFunction = scriptFunction;
            return this;
        }

        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService) {
            // Ignore Circuit Breaker
            final String fieldName = fieldType.name();
            if (BINARY_INDEX_FIELD_NAMES.contains(fieldName)) {
                assert numericType == null;
                return new BinaryDVIndexFieldData(indexSettings.getIndex(), fieldName);
            } else if (numericType != null) {
                return new SortedNumericDVIndexFieldData(indexSettings.getIndex(), fieldName, numericType);
            } else {
                return new SortedSetDVOrdinalsIndexFieldData(indexSettings, cache, fieldName, breakerService, scriptFunction);
            }
        }

    }

}
