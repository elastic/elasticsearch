/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfigTests;

import static org.elasticsearch.xpack.ml.inference.persistence.IndexRequestCreator.PRE_PACKAGED_MODELS;
import static org.hamcrest.Matchers.is;

public class IndexRequestCreatorTests extends ESTestCase {
    public void testCreatesAnIndexRequestWithOperationCreate() {
        TrainedModelConfig config = TrainedModelConfigTests.createTestInstance("modelId").build();

        IndexRequest requestWithoutIndexSpecified = IndexRequestCreator.create(config.getModelId(), "docId", config);
        assertThat(requestWithoutIndexSpecified.opType(), is(DocWriteRequest.OpType.CREATE));

        IndexRequest requestWithIndexSpecified = IndexRequestCreator.create(config.getModelId(), "docId", "index", config);
        assertThat(requestWithIndexSpecified.opType(), is(DocWriteRequest.OpType.CREATE));
    }

    public void testCreatesAnIndexRequestWithOperationIndexForPrePackagedModels() {
        for (String modelId : PRE_PACKAGED_MODELS) {
            TrainedModelConfig config = TrainedModelConfigTests.createTestInstance(modelId).build();

            IndexRequest requestWithoutIndexSpecified = IndexRequestCreator.create(config.getModelId(), "docId", config);
            assertThat(requestWithoutIndexSpecified.opType(), is(DocWriteRequest.OpType.INDEX));

            IndexRequest requestWithIndexSpecified = IndexRequestCreator.create(config.getModelId(), "docId", config);
            assertThat(requestWithIndexSpecified.opType(), is(DocWriteRequest.OpType.INDEX));
        }
    }
}
