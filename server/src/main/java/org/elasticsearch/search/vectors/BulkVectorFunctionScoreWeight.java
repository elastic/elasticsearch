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
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Weight implementation that enables bulk vector processing for KNN rescoring queries.
 * Extracts segment-specific documents from ScoreDoc array and creates bulk scorers.
 */
public class BulkVectorFunctionScoreWeight extends Weight {

    private final Weight subQueryWeight;
    private final AccessibleVectorSimilarityFloatValueSource valueSource;
    private final ScoreDoc[] scoreDocs;

    public BulkVectorFunctionScoreWeight(
        Query parent,
        Weight subQueryWeight,
        AccessibleVectorSimilarityFloatValueSource valueSource,
        ScoreDoc[] scoreDocs
    ) {
        super(parent);
        this.subQueryWeight = subQueryWeight;
        this.valueSource = valueSource;
        this.scoreDocs = scoreDocs;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        ScorerSupplier subQueryScorerSupplier = subQueryWeight.scorerSupplier(context);
        if (subQueryScorerSupplier == null) {
            return null;
        }

        // Extract documents belonging to this segment
        int[] segmentDocIds = extractSegmentDocuments(scoreDocs, context);
        if (segmentDocIds.length == 0) {
            return null; // No documents in this segment
        }

        return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
                // if asked for basic Scorer, delegate to the underlying subquery scorer
                return subQueryScorerSupplier.get(leadCost);
            }

            @Override
            public BulkScorer bulkScorer() throws IOException {
                // Always use BulkScorer when bulk processing is enabled
                BulkScorer subQueryBulkScorer = subQueryScorerSupplier.bulkScorer();
                return new BulkVectorScorer(subQueryBulkScorer, segmentDocIds, valueSource, context);
            }

            @Override
            public long cost() {
                return segmentDocIds.length;
            }
        };
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        // Find the document in our ScoreDoc array
        int globalDocId = doc + context.docBase;
        for (ScoreDoc scoreDoc : scoreDocs) {
            if (scoreDoc.doc == globalDocId) {
                // Compute explanation for this specific document
                try {
                    DirectIOVectorBatchLoader batchLoader = new DirectIOVectorBatchLoader();
                    float[] docVector = batchLoader.loadSingleVector(doc, context, valueSource.field());
                    float similarity = valueSource.similarityFunction().compare(valueSource.target(), docVector);

                    return Explanation.match(
                        similarity,
                        "bulk vector similarity score, computed with vector similarity function: " + valueSource.similarityFunction()
                    );
                } catch (Exception e) {
                    return Explanation.noMatch("Failed to compute vector similarity: " + e.getMessage());
                }
            }
        }
        return Explanation.noMatch("Document not in bulk processing set");
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

    private int[] extractSegmentDocuments(ScoreDoc[] scoreDocs, LeafReaderContext context) {
        List<Integer> segmentDocs = new ArrayList<>();
        int docBase = context.docBase;
        int maxDoc = docBase + context.reader().maxDoc();

        for (ScoreDoc scoreDoc : scoreDocs) {
            if (scoreDoc.doc >= docBase && scoreDoc.doc < maxDoc) {
                // Convert to segment-relative document ID
                segmentDocs.add(scoreDoc.doc - docBase);
            }
        }

        return segmentDocs.stream().mapToInt(Integer::intValue).toArray();
    }
}
