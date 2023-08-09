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
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Extracts query features, e.g. _scores, from the provided weights and featureNames.
 * For every document provided, this extractor iterates with the constructed scorers and collects the _score (if matched) for the
 * respective feature name.
 */
public class QueryFeatureExtractor implements FeatureExtractor {

    private final List<String> featureNames;
    private final List<Weight> weights;
    private final List<Scorer> scorers;
    private DisjunctionDISIApproximation rankerIterator;

    public QueryFeatureExtractor(List<String> featureNames, List<Weight> weights) {
        if (featureNames.size() != weights.size()) {
            throw new IllegalArgumentException("[featureNames] and [weights] must be the same size.");
        }
        this.featureNames = featureNames;
        this.weights = weights;
        this.scorers = new ArrayList<>(weights.size());
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
            if (scorer != null) {
                disiPriorityQueue.add(new DisiWrapper(scorer));
            }
            scorers.add(scorer);
        }
        rankerIterator = new DisjunctionDISIApproximation(disiPriorityQueue);
    }

    @Override
    public void addFeatures(Map<String, Object> featureMap, int docId) throws IOException {
        rankerIterator.advance(docId);
        for (int i = 0; i < featureNames.size(); i++) {
            Scorer scorer = scorers.get(i);
            // Do we have a scorer, and does it match the provided document?
            if (scorer != null && scorer.docID() == docId) {
                featureMap.put(featureNames.get(i), scorer.score());
            }
        }
    }

    @Override
    public List<String> featureNames() {
        return featureNames;
    }

}
