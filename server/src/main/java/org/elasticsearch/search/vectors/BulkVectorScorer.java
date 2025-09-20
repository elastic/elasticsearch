/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Map;

public class BulkVectorScorer extends BulkScorer {

    private final BulkScorer subQueryBulkScorer;
    private final int[] segmentDocIds;
    private final AccessibleVectorSimilarityFloatValueSource valueSource;
    private final LeafReaderContext context;

    // Bulk processing cache
    private Map<Integer, Float> precomputedScores;
    private boolean bulkProcessingCompleted = false;

    public BulkVectorScorer(
        BulkScorer subQueryBulkScorer,
        int[] segmentDocIds,
        AccessibleVectorSimilarityFloatValueSource valueSource,
        LeafReaderContext context
    ) {
        this.subQueryBulkScorer = subQueryBulkScorer;
        this.segmentDocIds = segmentDocIds;
        this.valueSource = valueSource;
        this.context = context;
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        // Perform bulk processing once if not already completed
        if (bulkProcessingCompleted == false) {
            performBulkVectorProcessing();
            bulkProcessingCompleted = true;
        }

        // Create bulk-aware collector wrapper
        BulkAwareCollector bulkCollector = new BulkAwareCollector(collector, precomputedScores);

        // Delegate to subquery bulk scorer with our bulk-aware collector
        return subQueryBulkScorer.score(bulkCollector, acceptDocs, min, max);
    }

    @Override
    public long cost() {
        return segmentDocIds.length;
    }

    private void performBulkVectorProcessing() throws IOException {
        // batch loading
        DirectIOVectorBatchLoader batchLoader = new DirectIOVectorBatchLoader();
        Map<Integer, float[]> vectorCache = batchLoader.loadSegmentVectors(segmentDocIds, context, valueSource.field());

        // batch similarity
        float[] similarities = BatchVectorSimilarity.computeBatchSimilarity(
            valueSource.target(),
            vectorCache,
            segmentDocIds,
            valueSource.similarityFunction()
        );

        // batch scoring
        precomputedScores = new java.util.HashMap<>();
        for (int i = 0; i < segmentDocIds.length; i++) {
            precomputedScores.put(segmentDocIds[i], similarities[i]);
        }
    }

    private static class BulkAwareCollector implements LeafCollector {

        private final LeafCollector delegate;
        private final Map<Integer, Float> precomputedScores;
        private final BulkProcessedScorable bulkScorable;

        BulkAwareCollector(LeafCollector delegate, Map<Integer, Float> precomputedScores) {
            this.delegate = delegate;
            this.precomputedScores = precomputedScores;
            this.bulkScorable = new BulkProcessedScorable();
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            // Set our bulk-aware scorer instead of the original
            bulkScorable.setDelegate(scorer);
            delegate.setScorer(bulkScorable);
        }

        @Override
        public void collect(int doc) throws IOException {
            // Set the current document for score retrieval
            bulkScorable.setCurrentDoc(doc);
            delegate.collect(doc);
        }

        /**
         * Scorable that provides pre-computed bulk scores
         */
        private class BulkProcessedScorable extends Scorable {
            private Scorable delegate;
            private int currentDoc = -1;

            public void setDelegate(Scorable delegate) {
                this.delegate = delegate;
            }

            public void setCurrentDoc(int doc) {
                this.currentDoc = doc;
            }

            @Override
            public float score() throws IOException {
                // Return pre-computed score if available
                Float precomputedScore = precomputedScores.get(currentDoc);
                if (precomputedScore != null) {
                    return precomputedScore;
                }

                // Fallback to delegate scorer
                return delegate != null ? delegate.score() : 0.0f;
            }
        }
    }
}
