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
import org.apache.lucene.util.packed.AppendingPackedLongBuffer;
import org.elasticsearch.ElasticsearchIllegalStateException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Records a "collect" stream for subsequent play-back
 */
public class RecordingBucketCollector extends BucketCollector  {

    List<PerSegmentCollects> perSegmentCollections = new ArrayList<>();
    private PerSegmentCollects currentCollection;
    private boolean recordingComplete;
    
    class PerSegmentCollects {
        AtomicReaderContext readerContext;
        AppendingPackedLongBuffer docs;
        AppendingPackedLongBuffer buckets;

        PerSegmentCollects(AtomicReaderContext readerContext) {
            this.readerContext = readerContext;
        }

        void collect(int doc, long owningBucketOrdinal) throws IOException {
            if (docs == null) {
                //TODO unclear what might be reasonable constructor args to pass to this collection
                // No way of accurately predicting how many docs will be collected 
                docs = new AppendingPackedLongBuffer();
            }
            docs.add(doc);
            if (buckets == null) {
                if (owningBucketOrdinal != 0) {
                    // Store all of the prior bucketOrds (which up until now have
                    // all been zero based)
                    buckets = new AppendingPackedLongBuffer();
                    for (int i = 0; i < docs.size() - 1; i++) {
                        buckets.add(0);
                    }
                    //record the new non-zero bucketID
                    buckets.add(owningBucketOrdinal);
                }
            } else {
                buckets.add(owningBucketOrdinal);
            }
        }
        void endCollect() {
            if (docs != null) {
                docs.freeze();
            }
            if (buckets != null) {
                buckets.freeze();
            }
        }

        boolean hasItems() {
            return docs != null;
        }

        void replay(BucketCollector collector) throws IOException {
            collector.setNextReader(readerContext);
            if (!hasItems()) {
                return;
            }           
            if (buckets == null) {
                // Collect all docs under the same bucket zero
                long[] docsBuffer = new long[(int) Math.min(1024, docs.size())];
                long pos = 0;
                while (pos < docs.size()) {
                    int numDocs = docs.get(pos, docsBuffer, 0, docsBuffer.length);
                    for (int i = 0; i < numDocs; i++) {
                        collector.collect((int) docsBuffer[i], 0);
                    }
                    pos += numDocs;
                }
            } else {
                assert docs.size() == buckets.size();
                // Collect all docs and parallel collection of buckets
                long[] docsBuffer = new long[(int) Math.min(1024, docs.size())];
                long[] bucketsBuffer = new long[(int) Math.min(1024, buckets.size())];
                long pos = 0;
                while (pos < docs.size()) {
                    int numDocs = docs.get(pos, docsBuffer, 0, docsBuffer.length);
                    int numBuckets = buckets.get(pos, bucketsBuffer, 0, bucketsBuffer.length);
                    // TODO not sure why numDocs and numBuckets differs, but
                    // they do and this min statement was required!
                    for (int i = 0; i < Math.min(numDocs, numBuckets); i++) {
                        collector.collect((int) docsBuffer[i], bucketsBuffer[i]);
                    }
                    pos += numDocs;
                }
            }
        }
    }
    
    @Override
    public void setNextReader(AtomicReaderContext reader) {
        if(recordingComplete){
            // The way registration works for listening on reader changes we have the potential to be called > once
            // TODO fixup the aggs framework so setNextReader calls are delegated to child aggs and not reliant on 
            // registering a listener.
            return;
        }
        stowLastSegmentCollection();
        currentCollection = new PerSegmentCollects(reader);
    }

    private void stowLastSegmentCollection() {
        if (currentCollection != null) {
            if (currentCollection.hasItems()) {
                currentCollection.endCollect();
                perSegmentCollections.add(currentCollection);
            }
            currentCollection = null;
        }
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        currentCollection.collect(doc, owningBucketOrdinal);
    }

    /*
     * Allows clients to replay a stream of collected items. 
     * 
     */
    public void replayCollection(BucketCollector collector) throws IOException{
        for (PerSegmentCollects collection : perSegmentCollections) {
            collection.replay(collector);
        }        
        collector.postCollection();
    }

    @Override
    public void postCollection() throws IOException {
        recordingComplete = true;
        stowLastSegmentCollection();
    }

    @Override
    public void gatherAnalysis(BucketAnalysisCollector analysisCollector, long bucketOrdinal) {
        throw new ElasticsearchIllegalStateException("gatherAnalysis not supported");
    }    
}
