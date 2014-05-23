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
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Records a "collect" stream for subsequent play-back
 */
public class RecordingBucketCollector extends BucketCollector implements Releasable {

    protected static final int INITIAL_CAPACITY = 50; // TODO sizing
    protected IntArray docs;
    protected IntArray readerIds;
    private LongArray buckets;
    protected int collectedItemNumber = 0;
    int readerNum = -1;    
    ArrayList<AtomicReaderContext> readers = new ArrayList<AtomicReaderContext>();
    private BigArrays bigArrays;
    
    public RecordingBucketCollector(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        docs=bigArrays.newIntArray(INITIAL_CAPACITY); //parent.estimatedBucketCount won't help - need to size for number of docs, not buckets so impossible to predict without executing query
        readerIds = bigArrays.newIntArray(INITIAL_CAPACITY);
        //Create the buckets array lazily - we may be collecting all-zero bucket Ords in many cases due to the wrapping that goes on with single-bucket Aggs  
    }
    
    
    @Override
    public void setNextReader(AtomicReaderContext reader) {
        readerNum = readers.size();
        readers.add(reader);
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        // Record all docIDs and the associated buckets in respective arrays
        docs = bigArrays.grow(docs, collectedItemNumber + 1);
        readerIds = bigArrays.grow(readerIds, collectedItemNumber + 1);
        docs.set(collectedItemNumber, doc);
        if (buckets == null) {
            if (owningBucketOrdinal != 0) {
                buckets = bigArrays.newLongArray(INITIAL_CAPACITY);
                // Store all of the prior bucketOrds (which up until now have
                // been zero based)
                buckets = bigArrays.grow(buckets, collectedItemNumber + 1);
                for (int i = 0; i < collectedItemNumber; i++) {
                    buckets.set(i, 0);
                }
                //record the new non-zero bucketID
                buckets.set(collectedItemNumber, owningBucketOrdinal);
            }
        } else {
            buckets = bigArrays.grow(buckets, collectedItemNumber + 1);
            buckets.set(collectedItemNumber, owningBucketOrdinal);
        }
        readerIds.set(collectedItemNumber, readerNum);
        collectedItemNumber++;
    }


    @Override
    public void close() {
        Releasables.close(docs, buckets, readerIds);
    }

    /*
     * Allows clients to replay a stream of collected items. 
     * 
     */
    public void replayCollection(BucketCollector collector) throws IOException{
        int readerId=-1;
        for (int i = 0; i < collectedItemNumber; i++) {
            long bucket = buckets==null?0:buckets.get(i);
            int doc = docs.get(i);
            int readerContext = readerIds.get(i);
            // establish the context for the doc id before asking child aggs
            // to collect it
            if (readerContext != readerId) {
                readerId = readerContext;
                collector.setNextReader(readers.get(readerId));
            }
            collector.collect(doc, bucket);
        }
        collector.postCollection();
    }

    @Override
    public void postCollection() throws IOException {
    }


    @Override
    public void gatherAnalysis(BucketAnalysisCollector analysisCollector, long bucketOrdinal) {
        throw new ElasticsearchIllegalStateException("gatherAnalysis not supported");
    }
    
}
