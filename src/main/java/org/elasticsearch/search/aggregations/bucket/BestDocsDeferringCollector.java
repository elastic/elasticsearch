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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * A {@link DeferringBucketCollector} that identifies the top-scoring documents
 * in search results and then passes only these on to nested collectors. This
 * implementation is only for use with a single bucket aggregation.
 * "Top-scoring" means the Lucene score but subclasses can provide an
 * alternative {@link TopDocsCollector} that uses other criteria.
 */
public class BestDocsDeferringCollector extends DeferringBucketCollector implements ScorerAware {

    protected TopDocsCollector<? extends ScoreDoc> topDocsCollector;
    private final List<PerSegmentCollects> perSegmentCollections = new ArrayList<>();
    private PerSegmentCollects currentCollection;
    protected Scorer currentScorer;
    private boolean collectCompleted;
    private int matchedDocCount = 0;

    public BestDocsDeferringCollector(BucketCollector deferred, AggregationContext context, int shardSize) {
        super(deferred, context);
        topDocsCollector = createTopDocsCollector(shardSize);
        currentScorer = context.currentScorer();
        // Keep abreast of scorer changes
        context.registerScorerAware(this);
    }

    // Designed to be overridden by subclasses that may score docs by criteria
    // other than Lucene score
    protected TopDocsCollector<? extends ScoreDoc> createTopDocsCollector(int size) {
        return TopScoreDocCollector.create(size, context.scoreDocsInOrder());
    }

    class PerSegmentCollects extends Scorer {
        private AtomicReaderContext readerContext;
        int maxDocId = Integer.MIN_VALUE;
        private float currentScore;
        private int currentDocId = -1;

        PerSegmentCollects(AtomicReaderContext readerContext) {
            // The publisher behaviour for Reader/Scorer listeners triggers a
            // call to this constructor with a null scorer so we can't call
            // scorer.getWeight() and pass the Weight to our base class.
            // However, passing null seems to have no adverse effects here...
            super(null);
            this.readerContext = readerContext;
        }

        public void replayRelatedMatches(ScoreDoc[] sd) throws IOException {
            // Need to set AggregationContext otherwise ValueSources in aggs
            // don't read any values
            context.setNextReader(readerContext);
            deferred.setNextReader(readerContext);
            currentScore = 0;
            currentDocId = -1;
            context.setScorer(this);
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
                    deferred.collect(rebased, 0);
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
            topDocsCollector.collect(docId);
            maxDocId = Math.max(maxDocId, docId);
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        if (collectCompleted) {
            return;
        }
        currentCollection = new PerSegmentCollects(reader);
        perSegmentCollections.add(currentCollection);
        try {
            topDocsCollector.setNextReader(reader);
        } catch (IOException e) {
            throw new ElasticsearchException("IOException collecting best scoring results", e);
        }
    }

    @Override
    public void collect(int docId, long bucketOrdinal) throws IOException {
        if (bucketOrdinal != 0) {
            throw new ElasticsearchIllegalArgumentException("Bucket ordinals must all be zero, received " + bucketOrdinal);
        }
        currentCollection.collect(docId);
    }

    @Override
    public void postCollection() throws IOException {
        collectCompleted = true;
    }

    @Override
    public void close() throws ElasticsearchException {
    }

    @Override
    public void prepareSelectedBuckets(long... survivingBucketOrds) {
        TopDocs topDocs = topDocsCollector.topDocs();
        ScoreDoc[] sd = topDocs.scoreDocs;
        matchedDocCount = sd.length;
        // Sort the top matches by docID for the benefit of deferred collector
        Arrays.sort(sd, new Comparator<ScoreDoc>() {
            @Override
            public int compare(ScoreDoc o1, ScoreDoc o2) {
                return o1.doc - o2.doc;
            }
        });
        try {
            for (PerSegmentCollects perSegDocs : perSegmentCollections) {
                perSegDocs.replayRelatedMatches(sd);
            }
            deferred.postCollection();
        } catch (IOException e) {
            throw new ElasticsearchException("IOException collecting best scoring results", e);
        }
    }

    @Override
    public void gatherAnalysis(BucketAnalysisCollector analysisCollector, long bucketOrdinal) {
        deferred.gatherAnalysis(analysisCollector, bucketOrdinal);
    }

    @Override
    public void setScorer(Scorer scorer) {
        try {
            currentScorer = scorer;
            topDocsCollector.setScorer(scorer);
        } catch (IOException e) {
            throw new ElasticsearchException("IO error setting scorer", e);
        }

    }

    public long getDocCount() {
        return matchedDocCount;
    }

}
