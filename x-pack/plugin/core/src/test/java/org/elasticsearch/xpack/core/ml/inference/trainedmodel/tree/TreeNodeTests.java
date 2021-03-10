/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

public class TreeNodeTests extends AbstractSerializingTestCase<TreeNode> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected TreeNode doParseInstance(XContentParser parser) throws IOException {
        return TreeNode.fromXContent(parser, lenient).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected TreeNode createTestInstance() {
        Integer lft = randomBoolean() ? null : randomInt(100);
        Integer rgt = randomBoolean() ? randomInt(100) : null;
        Double threshold = lft != null || randomBoolean() ? randomDouble() : null;
        Integer featureIndex = lft != null || randomBoolean() ? randomInt(100) : null;
        return createRandom(randomInt(100),
            lft,
            rgt,
            threshold,
            featureIndex,
            randomBoolean() ? null : randomFrom(Operator.values())).build();
    }

    public static TreeNode createRandomLeafNode(double internalValue) {
        return TreeNode.builder(randomInt(100))
            .setDefaultLeft(randomBoolean() ? null : randomBoolean())
            .setNumberSamples(randomNonNegativeLong())
            .setLeafValue(Collections.singletonList(internalValue))
            .build();
    }

    public static TreeNode.Builder createRandom(int nodeId,
                                                Integer left,
                                                Integer right,
                                                Double threshold,
                                                Integer featureIndex,
                                                Operator operator) {
        return TreeNode.builder(nodeId)
            .setLeafValue(left == null ? Collections.singletonList(randomDouble()) : null)
            .setDefaultLeft(randomBoolean() ? null : randomBoolean())
            .setLeftChild(left)
            .setRightChild(right)
            .setNumberSamples(randomNonNegativeLong())
            .setThreshold(threshold)
            .setOperator(operator)
            .setSplitFeature(randomBoolean() ? null : randomInt())
            .setSplitGain(randomBoolean() ? null : randomDouble())
            .setSplitFeature(featureIndex);
    }

    @Override
    protected Writeable.Reader<TreeNode> instanceReader() {
        return TreeNode::new;
    }
}
