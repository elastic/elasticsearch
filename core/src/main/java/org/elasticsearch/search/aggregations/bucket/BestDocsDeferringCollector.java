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

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * A specialization of {@link DeferringBucketCollector} that collects all
 * matches and then replays only the top scoring documents to child
 * aggregations. The method
 * {@link BestDocsDeferringCollector#createTopDocsCollector(int)} is designed to
 * be overridden and allows subclasses to choose a custom collector
 * implementation for determining the top N matches.
 * 
 */

public class BestDocsDeferringCollector extends DeferringBucketCollector implements Releasable {
    final List<PerSegmentCollects> entries = new ArrayList<>();
    BucketCollector deferred;
    ObjectArray<PerParentBucketSamples> perBucketSamples;
    private int shardSize;
    private PerSegmentCollects perSegCollector;
    private final BigArrays bigArrays;

    /**
     * Sole constructor.
     * 
     * @param shardSize
     *            The number of top-scoring docs to collect for each bucket
     * @param bigArrays
     */
    public BestDocsDeferringCollector(int shardSize, BigArrays bigArrays) {
        this.shardSize = shardSize;
        this.bigArrays = bigArrays;
        perBucketSamples = bigArrays.newObjectArray(1);
    }



    @Override
    public boolean needsScores() {
        return true;
    }

    /** Set the deferred collectors. */
    public void setDeferredCollector(Iterable<BucketCollector> deferredCollectors) {
        this.deferred = BucketCollector.wrap(deferredCollectors);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        perSegCollector = new PerSegmentCollects(ctx);
        entries.add(perSegCollector);

        // Deferring collector
        return new LeafBucketCollector() {
            @Override
            public void setScorer(Scorer scorer) throws IOException {
                perSegCollector.setScorer(scorer);
            }

            @Override
            public void collect(int doc, long bucket) throws IOException {
                perSegCollector.collect(doc, bucket);
            }
        };
    }

    // Designed to be overridden by subclasses that may score docs by criteria
    // other than Lucene score
    protected TopDocsCollector<? extends ScoreDoc> createTopDocsCollector(int size) throws IOException {
        return TopScoreDocCollector.create(size);
    }

    @Override
    public void preCollection() throws IOException {
    }

    @Override
    public void postCollection() throws IOException {
        runDeferredAggs();
    }


    @Override
    public void prepareSelectedBuckets(long... selectedBuckets) throws IOException {
        // no-op - deferred aggs processed in postCollection call
    }

    private void runDeferredAggs() throws IOException {
        deferred.preCollection();

        List<ScoreDoc> allDocs = new ArrayList<>(shardSize);
        for (int i = 0; i < perBucketSamples.size(); i++) {
            PerParentBucketSamples perBucketSample = perBucketSamples.get(i);
            if (perBucketSample == null) {
                continue;
            }
            perBucketSample.getMatches(allDocs);
        }
        
        // Sort the top matches by docID for the benefit of deferred collector
        ScoreDoc[] docsArr = allDocs.toArray(new ScoreDoc[allDocs.size()]);
        Arrays.sort(docsArr, new Comparator<ScoreDoc>() {
             @Override
             public int compare(ScoreDoc o1, ScoreDoc o2) {
                 if(o1.doc == o2.doc){
                     return o1.shardIndex - o2.shardIndex;                    
                 }
                 return o1.doc - o2.doc;
             }
         });
        try {
            for (PerSegmentCollects perSegDocs : entries) {
                perSegDocs.replayRelatedMatches(docsArr);
            }
        } catch (IOException e) {
            throw new ElasticsearchException("IOException collecting best scoring results", e);
        }
        deferred.postCollection();
    }

    class PerParentBucketSamples {
        private LeafCollector currentLeafCollector;
        private TopDocsCollector<? extends ScoreDoc> tdc;
        private long parentBucket;
        private int matchedDocs;

        public PerParentBucketSamples(long parentBucket, Scorer scorer, LeafReaderContext readerContext) {
            try {
                this.parentBucket = parentBucket;
                tdc = createTopDocsCollector(shardSize);
                currentLeafCollector = tdc.getLeafCollector(readerContext);
                setScorer(scorer);
            } catch (IOException e) {
                throw new ElasticsearchException("IO error creating collector", e);
            }
        }

        public void getMatches(List<ScoreDoc> allDocs) {
            TopDocs topDocs = tdc.topDocs();
            ScoreDoc[] sd = topDocs.scoreDocs;
            matchedDocs = sd.length;
            for (ScoreDoc scoreDoc : sd) {
                // A bit of a hack to (ab)use shardIndex property here to
                // hold a bucket ID but avoids allocating extra data structures
                // and users should have bigger concerns if bucket IDs
                // exceed int capacity..
                scoreDoc.shardIndex = (int) parentBucket;
            }
            allDocs.addAll(Arrays.asList(sd));
        }

        public void collect(int doc) throws IOException {
            currentLeafCollector.collect(doc);
        }

        public void setScorer(Scorer scorer) throws IOException {
            currentLeafCollector.setScorer(scorer);
        }

        public void changeSegment(LeafReaderContext readerContext) throws IOException {
            currentLeafCollector = tdc.getLeafCollector(readerContext);
        }

        public int getDocCount() {
            return matchedDocs;
        }
    }

    class PerSegmentCollects extends Scorer {
        private LeafReaderContext readerContext;
        int maxDocId = Integer.MIN_VALUE;
        private float currentScore;
        private int currentDocId = -1;
        private Scorer currentScorer;

        PerSegmentCollects(LeafReaderContext readerContext) throws IOException {
            // The publisher behaviour for Reader/Scorer listeners triggers a
            // call to this constructor with a null scorer so we can't call
            // scorer.getWeight() and pass the Weight to our base class.
            // However, passing null seems to have no adverse effects here...
            super(null);
            this.readerContext = readerContext;
            for (int i = 0; i < perBucketSamples.size(); i++) {
                PerParentBucketSamples perBucketSample = perBucketSamples.get(i);
                if (perBucketSample == null) {
                    continue;
                }
                perBucketSample.changeSegment(readerContext);
            }
        }

        public void setScorer(Scorer scorer) throws IOException {
            this.currentScorer = scorer;
            for (int i = 0; i < perBucketSamples.size(); i++) {
                PerParentBucketSamples perBucketSample = perBucketSamples.get(i);
                if (perBucketSample == null) {
                    continue;
                }
                perBucketSample.setScorer(scorer);
            }
        }

        public void replayRelatedMatches(ScoreDoc[] sd) throws IOException {
            final LeafBucketCollector leafCollector = deferred.getLeafCollector(readerContext);
            leafCollector.setScorer(this);

            currentScore = 0;
            currentDocId = -1;
            if (maxDocId < 0) {
                return;
            }
            for (ScoreDoc scoreDoc : sd) {
                // Doc ids from TopDocCollector are root-level Reader so
                // need rebasing
                int rebased = scoreDoc.doc - readerContext.docBase;
                if ((rebased >= 0) && (rebased <= maxDocId)) {
                    currentScore = scoreDoc.score;
                    currentDocId = rebased;
                    // We stored the bucket ID in Lucene's shardIndex property
                    // for convenience. 
                    leafCollector.collect(rebased, scoreDoc.shardIndex);
                }
            }

        }

        @Override
        public float score() throws IOException {
            return currentScore;
        }

        @Override
        public int freq() throws IOException {
            throw new ElasticsearchException("This caching scorer implementation only implements score() and docID()");
        }

        @Override
        public int docID() {
            return currentDocId;
        }

        @Override
        public int nextDoc() throws IOException {
            throw new ElasticsearchException("This caching scorer implementation only implements score() and docID()");
        }

        @Override
        public int advance(int target) throws IOException {
            throw new ElasticsearchException("This caching scorer implementation only implements score() and docID()");
        }

        @Override
        public long cost() {
            throw new ElasticsearchException("This caching scorer implementation only implements score() and docID()");
        }

        public void collect(int docId, long parentBucket) throws IOException {
            perBucketSamples = bigArrays.grow(perBucketSamples, parentBucket + 1);
            PerParentBucketSamples sampler = perBucketSamples.get((int) parentBucket);
            if (sampler == null) {
                sampler = new PerParentBucketSamples(parentBucket, currentScorer, readerContext);
                perBucketSamples.set((int) parentBucket, sampler);
            }
            sampler.collect(docId);
            maxDocId = Math.max(maxDocId, docId);
        }
    }


    public int getDocCount(long parentBucket) {
        PerParentBucketSamples sampler = perBucketSamples.get((int) parentBucket);
        if (sampler == null) {
            // There are conditions where no docs are collected and the aggs
            // framework still asks for doc count.
            return 0;
        }
        return sampler.getDocCount();
    }

    @Override
    public void close() throws ElasticsearchException {
        Releasables.close(perBucketSamples);
    }

}
