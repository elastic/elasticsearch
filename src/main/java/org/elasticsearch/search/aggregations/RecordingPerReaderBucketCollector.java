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
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.packed.AppendingDeltaPackedLongBuffer;
import org.apache.lucene.util.packed.AppendingPackedLongBuffer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
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

    // A scorer used for the deferred collection mode to handle any child aggs asking for scores that are not 
    // recorded.
    static final Scorer unavailableScorer = new Scorer(null){
        private final String MSG = "A limitation of the " + SubAggCollectionMode.BREADTH_FIRST.parseField().getPreferredName()
                + " collection mode is that scores cannot be buffered along with document IDs";

        @Override
        public float score() throws IOException {
            throw new ElasticsearchParseException(MSG);
        }

        @Override
        public int freq() throws IOException {
            throw new ElasticsearchParseException(MSG);
        }

        @Override
        public int advance(int arg0) throws IOException {
            throw new ElasticsearchParseException(MSG);
        }

        @Override
        public long cost() {
            throw new ElasticsearchParseException(MSG);
        }

        @Override
        public int docID() {
            throw new ElasticsearchParseException(MSG);
        }

        @Override
        public int nextDoc() throws IOException {
            throw new ElasticsearchParseException(MSG);
        }};

    final List<PerSegmentCollects> perSegmentCollections = new ArrayList<>();
    private PerSegmentCollects currentCollection;
    private boolean recordingComplete;

    private IndexReaderContext topReader;
    
    static class PerSegmentCollects {
        IndexReaderContext topReaderContext;
        AtomicReaderContext readerContext;
        AppendingPackedLongBuffer docs;
        AppendingPackedLongBuffer buckets;
        int lastDocId = 0;

        PerSegmentCollects(AtomicReaderContext readerContext, IndexReaderContext topReaderContext) {
            this.readerContext = readerContext;
            this.topReaderContext = topReaderContext;
        }

        void collect(int doc, long owningBucketOrdinal) throws IOException {
            if (docs == null) {
                // TODO unclear what might be reasonable constructor args to pass to this collection
                // No way of accurately predicting how many docs will be collected 
                docs = new AppendingPackedLongBuffer();
            }
            // Store as delta-encoded for better compression
            docs.add(doc - lastDocId);
            lastDocId = doc;
            if (buckets == null) {
                if (owningBucketOrdinal != 0) {
                    // Store all of the prior bucketOrds (which up until now have
                    // all been zero based)
                    buckets = new AppendingPackedLongBuffer();
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
            lastDocId = 0;
            collector.setNextReader(this.topReaderContext);
            collector.setNextReader(readerContext);
            collector.setScorer(unavailableScorer);
            if (!hasItems()) {
                return;
            }
            if (buckets == null) {
                final AppendingDeltaPackedLongBuffer.Iterator docsIter = docs.iterator();
                while (docsIter.hasNext()) {
                    lastDocId += (int) docsIter.next();
                    collector.collect(lastDocId, 0);
                }
            } else {
                assert docs.size() == buckets.size();
                final AppendingDeltaPackedLongBuffer.Iterator docsIter = docs.iterator();
                final AppendingDeltaPackedLongBuffer.Iterator bucketsIter = buckets.iterator();
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
        stowLastSegmentCollection();
        currentCollection = new PerSegmentCollects(reader, topReader);
    }
    
    @Override
    public void setNextReader(IndexReaderContext reader) {
        this.topReader = reader;
    }
    
    @Override
    public void setScorer(Scorer scorer) {
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
