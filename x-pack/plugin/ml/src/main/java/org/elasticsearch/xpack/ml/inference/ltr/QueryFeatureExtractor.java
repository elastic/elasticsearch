/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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

    private final DisiPriorityQueue subScorers;
    private DisjunctionDISIApproximation approximation;

    public QueryFeatureExtractor(List<String> featureNames, List<Weight> weights) {
        if (featureNames.size() != weights.size()) {
            throw new IllegalArgumentException("[featureNames] and [weights] must be the same size.");
        }
        this.featureNames = featureNames;
        this.weights = weights;
        this.subScorers = DisiPriorityQueue.ofMaxSize(weights.size());
    }

    @Override
    public void setNextReader(LeafReaderContext segmentContext) throws IOException {
        Collection<FeatureDisiWrapper> disiWrappers = new ArrayList<>();
        subScorers.clear();
        for (int i = 0; i < weights.size(); i++) {
            var weight = weights.get(i);
            if (weight == null) {
                continue;
            }
            var scorerSupplier = weight.scorerSupplier(segmentContext);
            if (scorerSupplier != null) {
                var scorer = scorerSupplier.get(0L);
                if (scorer != null) {
                    FeatureDisiWrapper featureDisiWrapper = new FeatureDisiWrapper(scorer, featureNames.get(i));
                    subScorers.add(featureDisiWrapper);
                    disiWrappers.add(featureDisiWrapper);
                }
            }
        }
        approximation = subScorers.size() > 0 ? new DisjunctionDISIApproximation(disiWrappers, Long.MAX_VALUE) : null;
    }

    @Override
    public void addFeatures(Map<String, Object> featureMap, int docId) throws IOException {
        if (approximation == null || approximation.docID() > docId) {
            return;
        }
        if (approximation.docID() < docId) {
            approximation.advance(docId);
        }
        if (approximation.docID() != docId) {
            return;
        }
        var w = (FeatureDisiWrapper) approximation.topList();
        for (; w != null; w = (FeatureDisiWrapper) w.next) {
            if (w.twoPhaseView == null || w.twoPhaseView.matches()) {
                featureMap.put(w.featureName, w.scorable.score());
            }
        }
    }

    @Override
    public List<String> featureNames() {
        return featureNames;
    }

    private static class FeatureDisiWrapper extends DisiWrapper {
        final String featureName;

        FeatureDisiWrapper(Scorer scorer, String featureName) {
            super(scorer, false);
            this.featureName = featureName;
        }
    }
}
