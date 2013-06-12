/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/** {@link IndexFieldData} impl based on Lucene's doc values. Caching is done on the Lucene side. */
public abstract class DocValuesIndexFieldData {

    private final AtomicLong maxUniqueValueCount;
    protected final Index index;
    protected final Names fieldNames;

    public DocValuesIndexFieldData(Index index, Names fieldNames) {
        super();
        this.index = index;
        this.fieldNames = fieldNames;
        maxUniqueValueCount = new AtomicLong();
    }

    public final Names getFieldNames() {
        return fieldNames;
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

    public final long getHighestNumberOfSeenUniqueValues() {
        return maxUniqueValueCount.get();
    }

    void updateMaxUniqueValueCount(long uniqueValueCount) {
        while (true) {
            final long current = maxUniqueValueCount.get();
            if (current >= uniqueValueCount || maxUniqueValueCount.compareAndSet(current, uniqueValueCount)) {
                break;
            }
        }
    }

    public static class Builder implements IndexFieldData.Builder {

        private static final Set<String> BINARY_INDEX_FIELD_NAMES = ImmutableSet.of(UidFieldMapper.NAME, IdFieldMapper.NAME);
        private static final Set<String> NUMERIC_INDEX_FIELD_NAMES = ImmutableSet.of(TimestampFieldMapper.NAME);

        private NumericType numericType;

        public Builder numericType(NumericType type) {
            this.numericType = type;
            return this;
        }

        @Override
        public IndexFieldData<?> build(Index index, Settings indexSettings, Names fieldNames, FieldDataType type, IndexFieldDataCache cache) {
            final Settings fdSettings = type.getSettings();
            final Map<String, Settings> filter = fdSettings.getGroups("filter");
            if (filter != null && !filter.isEmpty()) {
                throw new ElasticSearchIllegalArgumentException("Doc values field data doesn't support filters [" + fieldNames.name() + "]");
            }

            if (BINARY_INDEX_FIELD_NAMES.contains(fieldNames.indexName())) {
                assert numericType == null;
                return new BinaryDVIndexFieldData(index, fieldNames);
            } else if (NUMERIC_INDEX_FIELD_NAMES.contains(fieldNames.indexName())) {
                assert !numericType.isFloatingPoint();
                return new NumericDVIndexFieldData(index, fieldNames);
            } else if (numericType != null) {
                return new SortedSetDVNumericIndexFieldData(index, fieldNames, numericType);
            } else {
                return new SortedSetDVBytesIndexFieldData(index, fieldNames);
            }
        }

    }

}
