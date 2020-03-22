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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.RamAccountingTermsEnum;

import java.io.IOException;

public abstract class AbstractIndexFieldData<FD extends LeafFieldData> extends AbstractIndexComponent implements IndexFieldData<FD> {

    private final String fieldName;
    protected final IndexFieldDataCache cache;

    public AbstractIndexFieldData(IndexSettings indexSettings, String fieldName, IndexFieldDataCache cache) {
        super(indexSettings);
        this.fieldName = fieldName;
        this.cache = cache;
    }

    @Override
    public String getFieldName() {
        return this.fieldName;
    }

    @Override
    public void clear() {
        cache.clear(fieldName);
    }

    @Override
    public FD load(LeafReaderContext context) {
        if (context.reader().getFieldInfos().fieldInfo(fieldName) == null) {
            // Some leaf readers may be wrapped and report different set of fields and use the same cache key.
            // If a field can't be found then it doesn't mean it isn't there,
            // so if a field doesn't exist then we don't cache it and just return an empty field data instance.
            // The next time the field is found, we do cache.
            return empty(context.reader().maxDoc());
        }

        try {
            FD fd = cache.load(context, this);
            return fd;
        } catch (Exception e) {
            if (e instanceof ElasticsearchException) {
                throw (ElasticsearchException) e;
            } else {
                throw new ElasticsearchException(e);
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
     * intends to take advantage of the CircuitBreaker.
     * <p>
     * Note that the .beforeLoad(...) and .afterLoad(...) methods must be
     * manually called.
     */
    public interface PerValueEstimator {

        /**
         * @return the number of bytes for the given term
         */
        long bytesPerValue(BytesRef term);

        /**
         * Execute any pre-loading estimations for the terms. May also
         * optionally wrap a {@link TermsEnum} in a
         * {@link RamAccountingTermsEnum}
         * which will estimate the memory on a per-term basis.
         *
         * @param terms terms to be estimated
         * @return A TermsEnum for the given terms
         */
        TermsEnum beforeLoad(Terms terms) throws IOException;

        /**
         * Possibly adjust a circuit breaker after field data has been loaded,
         * now that the actual amount of memory used by the field data is known
         *
         * @param termsEnum  terms that were loaded
         * @param actualUsed actual field data memory usage
         */
        void afterLoad(TermsEnum termsEnum, long actualUsed);
    }
}
