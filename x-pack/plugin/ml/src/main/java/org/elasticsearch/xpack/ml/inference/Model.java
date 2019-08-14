/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A model of {@link Tree} ensembles.
 *
 * All trees should be trained on the same features.
 * {@code featureMap} maps document field names to an index in
 * the feature vectors used by the trees.
 */
public class Model {

    private final List<Tree> trees;
    private final Map<String, Integer> featureMap;

    private Model(List<Tree> trees, Map<String, Integer> featureMap) {
        this.trees = Collections.unmodifiableList(trees);
        this.featureMap = featureMap;
    }

    public int numFeatures() {
        return featureMap.size();
    }

    public double predict(Map<String, Double> features) {
        List<Double> featureVec = docToFeatureVector(features);
        List<Double> predictions = trees.stream().map(tree -> tree.predict(featureVec)).collect(Collectors.toList());
        return mergePredictions(predictions);
    }

    List<List<Tree.Node>> trace(Map<String, Double> features) {
        List<Double> featureVec = docToFeatureVector(features);
        return trees.stream().map(tree -> tree.trace(featureVec)).collect(Collectors.toList());
    }

    double mergePredictions(List<Double> predictions) {
        return predictions.get(0); // TODO implement this
    }

    List<Double> docToFeatureVector(Map<String, Double> features) {
        List<Double> featureVec = Arrays.asList(new Double[featureMap.size()]);

        for (Map.Entry<String, Double> keyValue : features.entrySet()) {
            if (featureMap.containsKey(keyValue.getKey())) {
                featureVec.set(featureMap.get(keyValue.getKey()), keyValue.getValue());
            }
        }

        return featureVec;
    }

    public static ModelBuilder modelBuilder(Map<String, Integer> featureMap) {
        return new ModelBuilder(featureMap);
    }

    public static class ModelBuilder {
        private List<Tree> trees;
        private Map<String, Integer> featureMap;

        public ModelBuilder(Map<String, Integer> featureMap) {
            this.featureMap = featureMap;
            trees = new ArrayList<>();
        }

        public ModelBuilder addTree(Tree tree) {
            trees.add(tree);
            return this;
        }

        public Model build() {
            return new Model(trees, featureMap);
        }
    }

}
