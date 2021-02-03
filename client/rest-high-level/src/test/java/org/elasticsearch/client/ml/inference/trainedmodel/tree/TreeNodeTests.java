/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.trainedmodel.tree;

import org.elasticsearch.client.ml.job.config.Operator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Collections;

public class TreeNodeTests extends AbstractXContentTestCase<TreeNode> {

    @Override
    protected TreeNode doParseInstance(XContentParser parser) throws IOException {
        return TreeNode.fromXContent(parser).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected TreeNode createTestInstance() {
        Integer lft = randomBoolean() ? null : randomInt(100);
        Integer rgt = randomBoolean() ? randomInt(100) : null;
        Double threshold = lft != null || randomBoolean() ? randomDouble() : null;
        Integer featureIndex = lft != null || randomBoolean() ? randomInt(100) : null;
        return createRandom(randomInt(), lft, rgt, threshold, featureIndex, randomBoolean() ? null : randomFrom(Operator.values())).build();
    }

    public static TreeNode createRandomLeafNode(double internalValue) {
        return TreeNode.builder(randomInt(100))
            .setDefaultLeft(randomBoolean() ? null : randomBoolean())
            .setLeafValue(Collections.singletonList(internalValue))
            .setNumberSamples(randomNonNegativeLong())
            .build();
    }

    public static TreeNode.Builder createRandom(int nodeIndex,
                                                Integer left,
                                                Integer right,
                                                Double threshold,
                                                Integer featureIndex,
                                                Operator operator) {
        return TreeNode.builder(nodeIndex)
            .setLeafValue(left == null ? Collections.singletonList(randomDouble()) : null)
            .setDefaultLeft(randomBoolean() ? null : randomBoolean())
            .setLeftChild(left)
            .setRightChild(right)
            .setNumberSamples(randomBoolean() ? null : randomNonNegativeLong())
            .setThreshold(threshold)
            .setOperator(operator)
            .setSplitFeature(featureIndex)
            .setSplitGain(randomBoolean() ? null : randomDouble());
    }

}
