/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.EnsembleTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LangIdentNeuralNetworkTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;

public class TrainedModelTypeTests extends ESTestCase {

    public void testTypeFromTrainedModel() {
        {
            TrainedModel tm = randomFrom(
                TreeTests.createRandom(TargetType.CLASSIFICATION),
                EnsembleTests.createRandom(TargetType.CLASSIFICATION)
            );
            assertEquals(TrainedModelType.TREE_ENSEMBLE, TrainedModelType.typeFromTrainedModel(tm));
        }
        {
            TrainedModel tm = LangIdentNeuralNetworkTests.createRandom();
            assertEquals(TrainedModelType.LANG_IDENT, TrainedModelType.typeFromTrainedModel(tm));
        }
    }
}
