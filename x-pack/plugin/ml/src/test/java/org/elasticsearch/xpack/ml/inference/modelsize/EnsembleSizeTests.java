/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.EnsembleTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.EnsembleInferenceModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.EnsembleInferenceModelTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.TreeInferenceModel;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EnsembleSizeTests extends SizeEstimatorTestCase<EnsembleSize, EnsembleInferenceModel> {

    static EnsembleSize createRandom() {
        return new EnsembleSize(Stream.generate(TreeSizeTests::createRandom).limit(randomIntBetween(1, 100)).collect(Collectors.toList()),
            randomIntBetween(1, 10000),
            Stream.generate(() -> randomIntBetween(1, 10)).limit(randomIntBetween(1, 10)).collect(Collectors.toList()),
            randomIntBetween(0, 10),
            randomIntBetween(0, 10),
            randomIntBetween(0, 10)
        );
    }

    static EnsembleSize translateToEstimate(EnsembleInferenceModel ensemble) {
        TreeInferenceModel tree = (TreeInferenceModel)ensemble.getModels().get(0);
        int numClasses = Arrays.stream(tree.getNodes())
            .filter(TreeInferenceModel.Node::isLeaf)
            .map(n -> (TreeInferenceModel.LeafNode)n)
            .findFirst()
            .get()
            .getLeafValue()
            .length;
        return new EnsembleSize(
            ensemble.getModels().stream().map(m -> TreeSizeTests.translateToEstimate((TreeInferenceModel)m)).collect(Collectors.toList()),
            randomIntBetween(0, 10),
            Arrays.stream(ensemble.getFeatureNames()).map(String::length).collect(Collectors.toList()),
            ensemble.getOutputAggregator().expectedValueSize() == null ? 0 : ensemble.getOutputAggregator().expectedValueSize(),
            ensemble.getClassificationWeights() == null ? 0 : ensemble.getClassificationWeights().length,
            numClasses);
    }

    @Override
    protected EnsembleSize createTestInstance() {
        return createRandom();
    }

    @Override
    protected EnsembleSize doParseInstance(XContentParser parser) {
        return EnsembleSize.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    EnsembleInferenceModel generateTrueObject() {
        try {
            Ensemble model = EnsembleTests.createRandom();
            EnsembleInferenceModel inferenceModel = EnsembleInferenceModelTests.serializeFromTrainedModel(model);
            inferenceModel.rewriteFeatureIndices(Collections.emptyMap());
            return inferenceModel;
        } catch (IOException ex) {
            throw new ElasticsearchException(ex);
        }
    }

    @Override
    EnsembleSize translateObject(EnsembleInferenceModel originalObject) {
        return translateToEstimate(originalObject);
    }
}
