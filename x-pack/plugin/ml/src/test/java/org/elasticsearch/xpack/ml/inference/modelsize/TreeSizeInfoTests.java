/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.TreeInferenceModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.TreeInferenceModelTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;

import java.io.IOException;
import java.util.Arrays;

public class TreeSizeInfoTests extends SizeEstimatorTestCase<TreeSizeInfo, TreeInferenceModel> {

    static TreeSizeInfo createRandom() {
        return new TreeSizeInfo(randomIntBetween(1, 100), randomIntBetween(0, 100), randomIntBetween(0, 10));
    }

    static TreeSizeInfo translateToEstimate(TreeInferenceModel tree) {
        int numClasses = Arrays.stream(tree.getNodes())
            .filter(TreeInferenceModel.Node::isLeaf)
            .map(n -> (TreeInferenceModel.LeafNode) n)
            .findFirst()
            .get()
            .getLeafValue().length;
        return new TreeSizeInfo(
            (int) Arrays.stream(tree.getNodes()).filter(TreeInferenceModel.Node::isLeaf).count(),
            (int) Arrays.stream(tree.getNodes()).filter(t -> t.isLeaf() == false).count(),
            numClasses
        );
    }

    @Override
    protected TreeSizeInfo createTestInstance() {
        return createRandom();
    }

    @Override
    protected TreeSizeInfo doParseInstance(XContentParser parser) {
        return TreeSizeInfo.fromXContent(parser);
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
    TreeSizeInfo translateObject(TreeInferenceModel originalObject) {
        return translateToEstimate(originalObject);
    }
}
