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

import org.elasticsearch.client.ml.job.config.Operator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

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
            .setLeafValue(internalValue)
            .build();
    }

    public static TreeNode.Builder createRandom(int nodeIndex,
                                                Integer left,
                                                Integer right,
                                                Double threshold,
                                                Integer featureIndex,
                                                Operator operator) {
        return TreeNode.builder(nodeIndex)
            .setLeafValue(left == null ? randomDouble() : null)
            .setDefaultLeft(randomBoolean() ? null : randomBoolean())
            .setLeftChild(left)
            .setRightChild(right)
            .setThreshold(threshold)
            .setOperator(operator)
            .setSplitFeature(featureIndex)
            .setSplitGain(randomBoolean() ? null : randomDouble());
    }

}
