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

import java.io.IOException;
import java.util.Arrays;

/**
 * Filters a collection stream of docIds and related buckets using a sorted 
 * list of valid bucket ordinals.
 */
public class FilteringBucketCollector extends BucketCollector {
    
    private long[] sortedOrds;
    private BucketCollector delegate;

    /**
     * 
     * @param sortedBucketOrds the list of valid BucketOrds - this array must be sorted!
     * @param delegate The collector that will be called for any buckets listed in sortedBucketOrds
     */
    public FilteringBucketCollector(long[] sortedBucketOrds, BucketCollector delegate) {
        this.sortedOrds = sortedBucketOrds;
        this.delegate = delegate;
    }

    @Override
    public final void setNextReader(AtomicReaderContext reader) {
        delegate.setNextReader(reader);
    }

    @Override
    public final void collect(int docId, long bucketOrdinal) throws IOException {
        int pos = Arrays.binarySearch(sortedOrds, bucketOrdinal);
        if(pos>=0){
            delegate.collect(docId, bucketOrdinal);
        }        
    }

    @Override
    public final void postCollection() throws IOException {
        delegate.postCollection();
    }
}
