/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * Extension to {@link org.apache.lucene.search.VectorScorer} that can score in bulk
 */
public interface BulkScorableVectorValues {
    interface BulkVectorScorer extends VectorScorer {

        /**
         * Returns a {@link BulkScorer} scorer that can score in bulk the provided {@code matchingDocs}.
         */
        BulkScorer bulkScore(DocIdSetIterator matchingDocs) throws IOException;

        interface BulkScorer {
            /**
             * Scores up to {@code nextCount} docs in the provided {@code buffer}.
             */
            void nextDocsAndScores(int nextCount, Bits liveDocs, DocAndFloatFeatureBuffer buffer) throws IOException;
        }
    }
}
