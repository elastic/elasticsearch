/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.TreeInferenceModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.TreeInferenceModelTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;

import java.io.IOException;
import java.util.Arrays;


public class TreeSizeTests extends SizeEstimatorTestCase<TreeSize, TreeInferenceModel> {

    static TreeSize createRandom() {
        return new TreeSize(randomIntBetween(1, 100), randomIntBetween(0, 100), randomIntBetween(0, 10));
    }

    static TreeSize translateToEstimate(TreeInferenceModel tree) {
        int numClasses = Arrays.stream(tree.getNodes())
            .filter(TreeInferenceModel.Node::isLeaf)
            .map(n -> (TreeInferenceModel.LeafNode)n)
            .findFirst()
            .get()
            .getLeafValue()
            .length;
        return new TreeSize((int)Arrays.stream(tree.getNodes()).filter(TreeInferenceModel.Node::isLeaf).count(),
            (int)Arrays.stream(tree.getNodes()).filter(t -> t.isLeaf() == false).count(),
            numClasses);
    }

    @Override
    protected TreeSize createTestInstance() {
        return createRandom();
    }

    @Override
    protected TreeSize doParseInstance(XContentParser parser) {
        return TreeSize.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    TreeInferenceModel generateTrueObject() {
        try {
            return TreeInferenceModelTests.serializeFromTrainedModel(
                TreeTests.buildRandomTree(Arrays.asList(randomAlphaOfLength(10), randomAlphaOfLength(10)), 6)
            );
        } catch (IOException ex) {
            throw new ElasticsearchException(ex);
        }
    }

    @Override
    TreeSize translateObject(TreeInferenceModel originalObject) {
        return translateToEstimate(originalObject);
    }
}
