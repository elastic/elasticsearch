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
import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.io.IOException;

/**
 * Filters a collection stream of docIds and related buckets using a single 
 * valid bucket ordinal.
 */
public class FilteringSingleBucketCollector extends BucketCollector {
    
    private final long ord;
    private final BucketCollector delegate;

    /**
     * 
     * @param ord the only valid BucketOrd
     * @param delegate The collector that will be called for matches on bucket ord
     */
    public FilteringSingleBucketCollector(long ord, BucketCollector delegate) {
        this.ord = ord;
        this.delegate = delegate;
    }

    @Override
    public final void setNextReader(AtomicReaderContext reader) {
        delegate.setNextReader(reader);
    }

    @Override
    public final void collect(int docId, long bucketOrdinal) throws IOException {
        if (bucketOrdinal == ord) {
            delegate.collect(docId, 0); //rebased ordinal
        }
    }

    @Override
    public void gatherAnalysis(BucketAnalysisCollector analysisCollector, long bucketOrdinal){
        if (bucketOrdinal != ord) {
            throw new ElasticsearchIllegalArgumentException("Aggregation requested on a missing bucket #" + bucketOrdinal);
        }
        delegate.gatherAnalysis(analysisCollector, 0);
    }


    @Override
    public final void postCollection() throws IOException {
        delegate.postCollection();
    }
}
