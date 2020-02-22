/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.inference.trainedmodel.tree;

import org.elasticsearch.client.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;


public class TreeTests extends AbstractXContentTestCase<Tree> {

    @Override
    protected Tree doParseInstance(XContentParser parser) throws IOException {
        return Tree.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.startsWith("feature_names");
    }

    @Override
    protected Tree createTestInstance() {
        return createRandom();
    }

    public static Tree createRandom() {
        int numberOfFeatures = randomIntBetween(1, 10);
        List<String> featureNames = new ArrayList<>();
        for (int i = 0; i < numberOfFeatures; i++) {
            featureNames.add(randomAlphaOfLength(10));
        }
        return buildRandomTree(featureNames,  6, randomFrom(TargetType.values()));
    }

    public static Tree buildRandomTree(List<String> featureNames, int depth, TargetType targetType) {
        int maxFeatureIndex = featureNames.size() -1;
        Tree.Builder builder = Tree.builder();
        builder.setFeatureNames(featureNames);

        TreeNode.Builder node = builder.addJunction(0, randomInt(maxFeatureIndex), true, randomDouble());
        List<Integer> childNodes = List.of(node.getLeftChild(), node.getRightChild());

        for (int i = 0; i < depth -1; i++) {

            List<Integer> nextNodes = new ArrayList<>();
            for (int nodeId : childNodes) {
                if (i == depth -2) {
                    builder.addLeaf(nodeId, randomDouble());
                } else {
                    TreeNode.Builder childNode =
                        builder.addJunction(nodeId, randomInt(maxFeatureIndex), true, randomDouble());
                    nextNodes.add(childNode.getLeftChild());
                    nextNodes.add(childNode.getRightChild());
                }
            }
            childNodes = nextNodes;
        }
        List<String> categoryLabels = null;
        if (randomBoolean() && targetType.equals(TargetType.CLASSIFICATION)) {
            categoryLabels = Arrays.asList(generateRandomStringArray(randomIntBetween(1, 10), randomIntBetween(1, 10), false, false));
        }
        return builder.setClassificationLabels(categoryLabels)
            .setTargetType(targetType)
            .build();
    }

}
