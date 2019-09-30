/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TrainedModelProviderIT extends MlSingleNodeTestCase {

    private TrainedModelProvider trainedModelProvider;

    @Before
    public void createComponents() throws Exception {
        trainedModelProvider = new TrainedModelProvider(client(), xContentRegistry());
        waitForMlTemplates();
    }

    public void testPutTrainedModelConfig() throws Exception {
        String modelId = "test-put-trained-model-config";
        TrainedModelConfig config = buildTrainedModelConfig(modelId, 0);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
    }

    public void testPutTrainedModelConfigThatAlreadyExists() throws Exception {
        String modelId = "test-put-trained-model-config-exists";
        TrainedModelConfig config = buildTrainedModelConfig(modelId, 0);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(),
            equalTo(Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, modelId, 0)));
    }

    public void testGetTrainedModelConfig() throws Exception {
        String modelId = "test-get-trained-model-config";
        TrainedModelConfig config = buildTrainedModelConfig(modelId, 0);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(listener -> trainedModelProvider.getTrainedModel(modelId, 0, listener), getConfigHolder, exceptionHolder);
        assertThat(getConfigHolder.get(), is(not(nullValue())));
        assertThat(getConfigHolder.get(), equalTo(config));
    }

    public void testGetMissingTrainingModelConfig() throws Exception {
        String modelId = "test-get-missing-trained-model-config";
        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(listener -> trainedModelProvider.getTrainedModel(modelId, 0, listener), getConfigHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(),
            equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId, 0)));
    }

    private static TrainedModelConfig buildTrainedModelConfig(String modelId, long modelVersion) {
        return TrainedModelConfig.builder()
            .setCreatedBy("ml_test")
            .setDefinition(TreeTests.createRandom())
            .setDescription("trained model config for test")
            .setModelId(modelId)
            .setModelType("binary_decision_tree")
            .setModelVersion(modelVersion)
            .build(Version.CURRENT);
    }

    @Override
    public NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);

    }

}
