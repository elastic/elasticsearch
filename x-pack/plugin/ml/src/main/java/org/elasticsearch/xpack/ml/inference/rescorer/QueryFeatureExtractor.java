/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryFeatureExtractor implements FeatureExtractor {

    private final List<String> featureNames;
    private final List<Weight> weights;
    private final List<Scorer> scorers;
    private DisjunctionDISI rankerIterator;

    public QueryFeatureExtractor(List<String> featureNames, List<ParsedQuery> queries, SearchExecutionContext executionContext)
        throws IOException {
        this.featureNames = featureNames;
        this.weights = new ArrayList<>(namedQueries.size());
        this.scorers = new ArrayList<>(namedQueries.size());
        for (QueryExtractorBuilder namedQueryExtractorBuilder : namedQueryExtractorBuilders) {
            Query namedQuery = namedQueries.get(namedQueryExtractorBuilder.queryName());
            if (namedQuery != null) {
                weights.add(
                    executionContext.searcher().rewrite(namedQuery).createWeight(executionContext.searcher(), ScoreMode.COMPLETE, 1f)
                );
            } else {
                weights.add(null);
            }
        }
    }

    @Override
    public void setNextReader(LeafReaderContext segmentContext) throws IOException {
        DisiPriorityQueue disiPriorityQueue = new DisiPriorityQueue(weights.size());
        scorers.clear();
        for (Weight weight : weights) {
            if (weight == null) {
                scorers.add(null);
                continue;
            }
            Scorer scorer = weight.scorer(segmentContext);
            // Could we just skip all this if the scorer or weight is null???
            if (scorer != null) {
                disiPriorityQueue.add(new DisiWrapper(scorer));
            }
            scorers.add(scorer);
        }
        rankerIterator = new DisjunctionDISI(DocIdSetIterator.all(segmentContext.reader().maxDoc()), disiPriorityQueue);
    }

    @Override
    public void addFeatures(Map<String, Object> featureMap, int docId) throws IOException {
        rankerIterator.advance(docId);
        for (int i = 0; i < namedQueryExtractorBuilders.size(); i++) {
            Scorer scorer = scorers.get(i);
            if (scorer != null) {
                featureMap.put(namedQueryExtractorBuilders.get(i).featureName(), scorer.score());
            }
        }
    }

    @Override
    public List<String> featureNames() {
        return namedQueryExtractorBuilders.stream().map(QueryExtractorBuilder::featureName).toList();
    }

    /**
     * Helper to iterate scores based on if they match the passed document or not
     */
    private static class DisjunctionDISI extends DocIdSetIterator {
        private final DocIdSetIterator main;
        private final DisiPriorityQueue subIteratorsPriorityQueue;

        DisjunctionDISI(DocIdSetIterator main, DisiPriorityQueue subIteratorsPriorityQueue) {
            this.main = main;
            this.subIteratorsPriorityQueue = subIteratorsPriorityQueue;
        }

        @Override
        public int docID() {
            return main.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            int doc = main.nextDoc();
            advanceSubIterators(doc);
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            int docId = main.advance(target);
            advanceSubIterators(docId);
            return docId;
        }

        private void advanceSubIterators(int target) throws IOException {
            if (target == NO_MORE_DOCS || subIteratorsPriorityQueue.size() == 0) {
                return;
            }
            DisiWrapper top = subIteratorsPriorityQueue.top();
            while (top.doc < target) {
                top.doc = top.iterator.advance(target);
                top = subIteratorsPriorityQueue.updateTop();
            }
        }

        @Override
        public long cost() {
            return main.cost();
        }
    }
}
