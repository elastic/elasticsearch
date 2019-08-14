/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class ModelTests extends ESTestCase {

    public void testMergePredictions() {
        Model emptyModel = Model.modelBuilder(Collections.emptyMap()).build();

        List<Double> predictions = Arrays.asList(0.1, 0.2, 0.3);
        assertEquals(0.1, emptyModel.mergePredictions(predictions), 0.00001);
    }

    public void testDocToFeatureVector() {
        Map<String, Integer> featureMap = new HashMap<>();

        featureMap.put("price", 0);
        featureMap.put("number_of_wheels", 1);
        featureMap.put("phone_charger", 2);

        Map<String, Double> document = new HashMap<>();
        document.put("price", 1000.0);
        document.put("number_of_wheels", 4.0);
        document.put("phone_charger", 0.0);
        document.put("unrelated", 1.0);

        Model emptyModel = Model.modelBuilder(featureMap).build();
        List<Double> featureVector = emptyModel.docToFeatureVector(document);
        assertEquals(Arrays.asList(1000.0, 4.0, 0.0), featureVector);


        featureMap.put("missing_field", 3);
        emptyModel = Model.modelBuilder(featureMap).build();
        featureVector = emptyModel.docToFeatureVector(document);
        assertEquals(Arrays.asList(1000.0, 4.0, 0.0, null), featureVector);
    }

    public void testTrace() {
        int numFeatures = randomIntBetween(1, 4);
        int numTrees = randomIntBetween(1, 5);
        Model model = buildRandomModel(numFeatures, numTrees);

        Map<String, Double> doc = new HashMap<>();
        doc.put("a", randomDouble());
        doc.put("b", randomDouble());
        doc.put("c", randomDouble());
        doc.put("d", randomDouble());
        List<List<Tree.Node>> traces = model.trace(doc);
        assertThat(traces, hasSize(numTrees));
        for (var trace : traces) {
            assertThat(trace, not(empty()));
        }
    }

    public void testPredict() {
        Map<String, Integer> featureMap = new HashMap<>();
        featureMap.put("a", 0);
        featureMap.put("b", 1);

        Model.ModelBuilder modelBuilder = Model.modelBuilder(featureMap);

        // Simple tree
        Tree.TreeBuilder treeBuilder1 = Tree.TreeBuilder.newTreeBuilder();
        Tree.Node rootNode = treeBuilder1.addJunction(0, 0, true, 0.5);
        treeBuilder1.addLeaf(rootNode.leftChild, 0.1);
        treeBuilder1.addLeaf(rootNode.rightChild, 0.2);

        Tree.TreeBuilder treeBuilder2 = Tree.TreeBuilder.newTreeBuilder();
        Tree.Node rootNode2 = treeBuilder2.addJunction(0, 1, true, 0.5);
        treeBuilder2.addLeaf(rootNode2.leftChild, 0.1);
        treeBuilder2.addLeaf(rootNode2.rightChild, 0.2);

        modelBuilder.addTree(treeBuilder1.build());
        modelBuilder.addTree(treeBuilder2.build());

        Model model = modelBuilder.build();

        // this doc should result in prediction 0.1 for tree1 and 0.2 for tree2
        Map<String, Double> doc = new HashMap<>();
        doc.put("a", 0.4);
        doc.put("b", 0.7);

        double prediction = model.predict(doc);
        assertEquals(0.1, prediction, 0.00001);
    }

    private Model buildRandomModel(int numFeatures, int numTrees) {
        Map<String, Integer> featureMap = new HashMap<>();
        char fieldName = 'a';
        int index = 0;
        for (int i=0; i<numFeatures; i++) {
            featureMap.put(Character.toString(fieldName++), index++);
        }

        Model.ModelBuilder builder = Model.modelBuilder(featureMap);
        for (int i=0; i<numTrees; i++) {
            builder.addTree(TreeTests.buildRandomTree(numFeatures, randomIntBetween(3, 6)));
        }

        return builder.build();
    }
}
