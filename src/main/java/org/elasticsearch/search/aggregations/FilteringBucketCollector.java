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

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;

import java.io.IOException;

/**
 * Filters a collection stream of docIds and related buckets using a sorted 
 * list of valid bucket ordinals.
 */
public class FilteringBucketCollector extends BucketCollector implements Releasable {
    
    private final LongHash denseMap;
    private final BucketCollector delegate;
    
    /**
     * 
     * @param the  valid BucketOrds
     * @param delegate The collector that will be called for any buckets listed in sortedBucketOrds
     */
    public FilteringBucketCollector(long[] validBucketOrds, BucketCollector delegate, BigArrays bigArrays) {
        denseMap = new LongHash(validBucketOrds.length, bigArrays);
        for (int i = 0; i < validBucketOrds.length; i++) {
            denseMap.add(validBucketOrds[i]);
        }
        this.delegate = delegate;
    }

    @Override
    public final void setNextReader(AtomicReaderContext reader) {
        delegate.setNextReader(reader);
    }

    @Override
    public final void collect(int docId, long bucketOrdinal) throws IOException {
        long ordinal = denseMap.find(bucketOrdinal);
        if (ordinal >= 0) {
            delegate.collect(docId, ordinal);
        }
    }

    @Override
    public final void postCollection() throws IOException {
        delegate.postCollection();
    }

    @Override
    public void close() throws ElasticsearchException {
        Releasables.close(denseMap);
    }

    @Override
    public void gatherAnalysis(BucketAnalysisCollector analysisCollector, long bucketOrdinal){        
        long ordinal = denseMap.find(bucketOrdinal);
        if (ordinal >= 0) {
            delegate.gatherAnalysis(analysisCollector, ordinal);
        } else {
            throw new ElasticsearchIllegalArgumentException("Aggregation requested on a missing bucket #" + bucketOrdinal);
        }
    }
}
