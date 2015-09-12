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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;

/**
 */
public abstract class AbstractIndexFieldData<FD extends AtomicFieldData> extends AbstractIndexComponent implements IndexFieldData<FD> {

    private final MappedFieldType.Names fieldNames;
    protected final FieldDataType fieldDataType;
    protected final IndexFieldDataCache cache;

    public AbstractIndexFieldData(Index index, @IndexSettings Settings indexSettings, MappedFieldType.Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings);
        this.fieldNames = fieldNames;
        this.fieldDataType = fieldDataType;
        this.cache = cache;
    }

    @Override
    public MappedFieldType.Names getFieldNames() {
        return this.fieldNames;
    }

    @Override
    public FieldDataType getFieldDataType() {
        return fieldDataType;
    }

    @Override
    public void clear() {
        cache.clear(fieldNames.indexName());
    }

    @Override
    public void clear(IndexReader reader) {
        cache.clear(reader);
    }

    @Override
    public FD load(LeafReaderContext context) {
        if (context.reader().getFieldInfos().fieldInfo(fieldNames.indexName()) == null) {
            // If the field doesn't exist, then don't bother with loading and adding an empty instance to the field data cache
            return empty(context.reader().maxDoc());
        }

        try {
            FD fd = cache.load(context, this);
            return fd;
        } catch (Throwable e) {
            if (e instanceof ElasticsearchException) {
                throw (ElasticsearchException) e;
            } else {
                throw new ElasticsearchException(e.getMessage(), e);
            }
        }
    }

    /**
     * @param maxDoc of the current reader
     * @return an empty field data instances for field data lookups of empty segments (returning no values)
     */
    protected abstract FD empty(int maxDoc);

    /**
     * A {@code PerValueEstimator} is a sub-class that can be used to estimate
     * the memory overhead for loading the data. Each field data
     * implementation should implement its own {@code PerValueEstimator} if it
     * intends to take advantage of the MemoryCircuitBreaker.
     * <p/>
     * Note that the .beforeLoad(...) and .afterLoad(...) methods must be
     * manually called.
     */
    public interface PerValueEstimator {

        /**
         * @return the number of bytes for the given term
         */
        public long bytesPerValue(BytesRef term);

        /**
         * Execute any pre-loading estimations for the terms. May also
         * optionally wrap a {@link TermsEnum} in a
         * {@link RamAccountingTermsEnum}
         * which will estimate the memory on a per-term basis.
         *
         * @param terms terms to be estimated
         * @return A TermsEnum for the given terms
         * @throws IOException
         */
        public TermsEnum beforeLoad(Terms terms) throws IOException;

        /**
         * Possibly adjust a circuit breaker after field data has been loaded,
         * now that the actual amount of memory used by the field data is known
         *
         * @param termsEnum  terms that were loaded
         * @param actualUsed actual field data memory usage
         */
        public void afterLoad(TermsEnum termsEnum, long actualUsed);
    }
}
