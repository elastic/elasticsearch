/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class TrainedModelProviderTests extends ESTestCase {

    public void testDeleteModelStoredAsResource() {
        TrainedModelProvider trainedModelProvider = new TrainedModelProvider(mock(Client.class), xContentRegistry());
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        // Should be OK as we don't make any client calls
        trainedModelProvider.deleteTrainedModel("lang_ident_model_1", future);
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_CANNOT_DELETE_MODEL, "lang_ident_model_1")));
    }

    public void testPutModelThatExistsAsResource() {
        TrainedModelConfig config = TrainedModelConfigTests.createTestInstance("lang_ident_model_1").build();
        TrainedModelProvider trainedModelProvider = new TrainedModelProvider(mock(Client.class), xContentRegistry());
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        trainedModelProvider.storeTrainedModel(config, future);
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, "lang_ident_model_1")));
    }

    public void testGetModelThatExistsAsResource() throws Exception {
        TrainedModelProvider trainedModelProvider = new TrainedModelProvider(mock(Client.class), xContentRegistry());
        for(String modelId : TrainedModelProvider.MODELS_STORED_AS_RESOURCE) {
            PlainActionFuture<TrainedModelConfig> future = new PlainActionFuture<>();
            trainedModelProvider.getTrainedModel(modelId, true, future);
            TrainedModelConfig configWithDefinition = future.actionGet();

            assertThat(configWithDefinition.getModelId(), equalTo(modelId));
            assertThat(configWithDefinition.ensureParsedDefinition(xContentRegistry()).getModelDefinition(), is(not(nullValue())));

            PlainActionFuture<TrainedModelConfig> futureNoDefinition = new PlainActionFuture<>();
            trainedModelProvider.getTrainedModel(modelId, false, futureNoDefinition);
            TrainedModelConfig configWithoutDefinition = futureNoDefinition.actionGet();

            assertThat(configWithoutDefinition.getModelId(), equalTo(modelId));
            assertThat(configWithDefinition.ensureParsedDefinition(xContentRegistry()).getModelDefinition(), is(not(nullValue())));
        }
    }

    public void testGetModelThatExistsAsResourceButIsMissing() {
        TrainedModelProvider trainedModelProvider = new TrainedModelProvider(mock(Client.class), xContentRegistry());
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> trainedModelProvider.loadModelFromResource("missing_model", randomBoolean()));
        assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, "missing_model")));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }
}
