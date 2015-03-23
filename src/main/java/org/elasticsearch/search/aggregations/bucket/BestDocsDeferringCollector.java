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
import org.elasticsearch.ElasticsearchIllegalStateException;
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

public class BestDocsDeferringCollector extends DeferringBucketCollector {
    final List<PerSegmentCollects> entries = new ArrayList<>();
    BucketCollector deferred;
    TopDocsCollector<? extends ScoreDoc> tdc;
    boolean finished = false;
    private int shardSize;
    private PerSegmentCollects perSegCollector;
    private int matchedDocs;

    /**
     * Sole constructor.
     * 
     * @param shardSize
     */
    public BestDocsDeferringCollector(int shardSize) {
        this.shardSize = shardSize;
    }


    @Override
    public boolean needsScores() {
        return true;
    }

    /** Set the deferred collectors. */
    public void setDeferredCollector(Iterable<BucketCollector> deferredCollectors) {
        this.deferred = BucketCollector.wrap(deferredCollectors);
        try {
            tdc = createTopDocsCollector(shardSize);
        } catch (IOException e) {
            throw new ElasticsearchException("IO error creating collector", e);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        // finishLeaf();
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
                perSegCollector.collect(doc);
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
        finished = true;
    }

    /**
     * Replay the wrapped collector, but only on a selection of buckets.
     */
    @Override
    public void prepareSelectedBuckets(long... selectedBuckets) throws IOException {
        if (!finished) {
            throw new ElasticsearchIllegalStateException("Cannot replay yet, collection is not finished: postCollect() has not been called");
        }
        if (selectedBuckets.length > 1) {
            throw new ElasticsearchIllegalStateException("Collection only supported on a single bucket");
        }

        deferred.preCollection();

        TopDocs topDocs = tdc.topDocs();
        ScoreDoc[] sd = topDocs.scoreDocs;
        matchedDocs = sd.length;
        // Sort the top matches by docID for the benefit of deferred collector
        Arrays.sort(sd, new Comparator<ScoreDoc>() {
            @Override
            public int compare(ScoreDoc o1, ScoreDoc o2) {
                return o1.doc - o2.doc;
            }
        });
        try {
            for (PerSegmentCollects perSegDocs : entries) {
                perSegDocs.replayRelatedMatches(sd);
            }
            // deferred.postCollection();
        } catch (IOException e) {
            throw new ElasticsearchException("IOException collecting best scoring results", e);
        }
        deferred.postCollection();
    }

    class PerSegmentCollects extends Scorer {
        private LeafReaderContext readerContext;
        int maxDocId = Integer.MIN_VALUE;
        private float currentScore;
        private int currentDocId = -1;
        private LeafCollector currentLeafCollector;

        PerSegmentCollects(LeafReaderContext readerContext) throws IOException {
            // The publisher behaviour for Reader/Scorer listeners triggers a
            // call to this constructor with a null scorer so we can't call
            // scorer.getWeight() and pass the Weight to our base class.
            // However, passing null seems to have no adverse effects here...
            super(null);
            this.readerContext = readerContext;
            currentLeafCollector = tdc.getLeafCollector(readerContext);

        }

        public void setScorer(Scorer scorer) throws IOException {
            currentLeafCollector.setScorer(scorer);
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
                    leafCollector.collect(rebased, 0);
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

        public void collect(int docId) throws IOException {
            currentLeafCollector.collect(docId);
            maxDocId = Math.max(maxDocId, docId);
        }
    }


    public int getDocCount() {
        return matchedDocs;
    }

}
