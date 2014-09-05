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
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Records a "collect" stream for subsequent play-back using a per-segment
 * object to collect matches. Playback is fast because each collection
 * contains only the required matches for the current reader.
 */
public class RecordingPerReaderBucketCollector extends RecordingBucketCollector  {

    final List<PerSegmentCollects> perSegmentCollections = new ArrayList<>();
    private PerSegmentCollects currentCollection;
    private boolean recordingComplete;
    
    static class PerSegmentCollects {
        AtomicReaderContext readerContext;
        PackedLongValues.Builder docs;
        PackedLongValues.Builder buckets;
        int lastDocId = 0;

        PerSegmentCollects(AtomicReaderContext readerContext) {
            this.readerContext = readerContext;
        }

        void collect(int doc, long owningBucketOrdinal) throws IOException {
            if (docs == null) {
                // TODO unclear what might be reasonable constructor args to pass to this collection
                // No way of accurately predicting how many docs will be collected 
                docs = PackedLongValues.packedBuilder(PackedInts.COMPACT);
            }
            // Store as delta-encoded for better compression
            docs.add(doc - lastDocId);
            lastDocId = doc;
            if (buckets == null) {
                if (owningBucketOrdinal != 0) {
                    // Store all of the prior bucketOrds (which up until now have
                    // all been zero based)
                    buckets = PackedLongValues.packedBuilder(PackedInts.COMPACT);
                    for (int i = 0; i < docs.size() - 1; i++) {
                        buckets.add(0);
                    }
                    // record the new non-zero bucketID
                    buckets.add(owningBucketOrdinal);
                }
            } else {
                buckets.add(owningBucketOrdinal);
            }
        }
        void endCollect() {
        }

        boolean hasItems() {
            return docs != null;
        }

        void replay(BucketCollector collector) throws IOException {
            lastDocId = 0;
            collector.setNextReader(readerContext);
            if (!hasItems()) {
                return;
            }
            if (buckets == null) {
                final PackedLongValues.Iterator docsIter = docs.build().iterator();
                while (docsIter.hasNext()) {
                    lastDocId += (int) docsIter.next();
                    collector.collect(lastDocId, 0);
                }
            } else {
                assert docs.size() == buckets.size();
                final PackedLongValues.Iterator docsIter = docs.build().iterator();
                final PackedLongValues.Iterator bucketsIter = buckets.build().iterator();
                while (docsIter.hasNext()) {
                    lastDocId += (int) docsIter.next();
                    collector.collect(lastDocId, bucketsIter.next());
                }
            }
        }
    }
    
    public RecordingPerReaderBucketCollector(AggregationContext context) {
        // Call this method to achieve better compression in the recorded arrays of matches
        context.ensureScoreDocsInOrder();        
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
    @Override
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
    public void close() throws ElasticsearchException {
    }    
}
