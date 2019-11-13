/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.junit.Before;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.inference.loadingservice.LocalModelTests.buildClassification;
import static org.elasticsearch.xpack.ml.inference.loadingservice.LocalModelTests.buildRegression;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class ModelInferenceActionIT extends MlSingleNodeTestCase {

    private TrainedModelProvider trainedModelProvider;

    @Before
    public void createComponents() throws Exception {
        trainedModelProvider = new TrainedModelProvider(client(), xContentRegistry());
        waitForMlTemplates();
    }

    public void testInferModels() throws Exception {
        String modelId1 = "test-load-models-regression";
        String modelId2 = "test-load-models-classification";
        Map<String, String> oneHotEncoding = new HashMap<>();
        oneHotEncoding.put("cat", "animal_cat");
        oneHotEncoding.put("dog", "animal_dog");
        TrainedModelConfig config1 = buildTrainedModelConfigBuilder(modelId2)
            .setInput(new TrainedModelInput(Arrays.asList("field1", "field2")))
            .setDefinition(new TrainedModelDefinition.Builder()
                .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotEncoding)))
                .setTrainedModel(buildClassification(true))
                .setModelId(modelId1))
            .setVersion(Version.CURRENT)
            .setCreateTime(Instant.now())
            .setEstimatedOperations(0)
            .setEstimatedHeapMemory(0)
            .build();
        TrainedModelConfig config2 = buildTrainedModelConfigBuilder(modelId1)
            .setInput(new TrainedModelInput(Arrays.asList("field1", "field2")))
            .setDefinition(new TrainedModelDefinition.Builder()
                .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotEncoding)))
                .setTrainedModel(buildRegression())
                .setModelId(modelId2))
            .setVersion(Version.CURRENT)
            .setEstimatedOperations(0)
            .setEstimatedHeapMemory(0)
            .setCreateTime(Instant.now())
            .build();
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config1, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config2, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));


        List<Map<String, Object>> toInfer = new ArrayList<>();
        toInfer.add(new HashMap<>() {{
            put("foo", 1.0);
            put("bar", 0.5);
            put("categorical", "dog");
        }});
        toInfer.add(new HashMap<>() {{
            put("foo", 0.9);
            put("bar", 1.5);
            put("categorical", "cat");
        }});

        List<Map<String, Object>> toInfer2 = new ArrayList<>();
        toInfer2.add(new HashMap<>() {{
            put("foo", 0.0);
            put("bar", 0.01);
            put("categorical", "dog");
        }});
        toInfer2.add(new HashMap<>() {{
            put("foo", 1.0);
            put("bar", 0.0);
            put("categorical", "cat");
        }});

        // Test regression
        InferModelAction.Request request = new InferModelAction.Request(modelId1, toInfer, new RegressionConfig());
        InferModelAction.Response response = client().execute(InferModelAction.INSTANCE, request).actionGet();
        assertThat(response.getInferenceResults().stream().map(i -> ((SingleValueInferenceResults)i).value()).collect(Collectors.toList()),
            contains(1.3, 1.25));

        request = new InferModelAction.Request(modelId1, toInfer2, new RegressionConfig());
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();
        assertThat(response.getInferenceResults().stream().map(i -> ((SingleValueInferenceResults)i).value()).collect(Collectors.toList()),
            contains(1.65, 1.55));


        // Test classification
        request = new InferModelAction.Request(modelId2, toInfer, new ClassificationConfig(0));
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();
        assertThat(response.getInferenceResults()
                .stream()
                .map(i -> ((SingleValueInferenceResults)i).valueAsString())
                .collect(Collectors.toList()),
            contains("not_to_be", "to_be"));

        // Get top classes
        request = new InferModelAction.Request(modelId2, toInfer, new ClassificationConfig(2));
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();

        ClassificationInferenceResults classificationInferenceResults =
            (ClassificationInferenceResults)response.getInferenceResults().get(0);

        assertThat(classificationInferenceResults.getTopClasses().get(0).getClassification(), equalTo("not_to_be"));
        assertThat(classificationInferenceResults.getTopClasses().get(1).getClassification(), equalTo("to_be"));
        assertThat(classificationInferenceResults.getTopClasses().get(0).getProbability(),
            greaterThan(classificationInferenceResults.getTopClasses().get(1).getProbability()));

        classificationInferenceResults = (ClassificationInferenceResults)response.getInferenceResults().get(1);
        assertThat(classificationInferenceResults.getTopClasses().get(0).getClassification(), equalTo("to_be"));
        assertThat(classificationInferenceResults.getTopClasses().get(1).getClassification(), equalTo("not_to_be"));
        // they should always be in order of Most probable to least
        assertThat(classificationInferenceResults.getTopClasses().get(0).getProbability(),
            greaterThan(classificationInferenceResults.getTopClasses().get(1).getProbability()));

        // Test that top classes restrict the number returned
        request = new InferModelAction.Request(modelId2, toInfer2, new ClassificationConfig(1));
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();

        classificationInferenceResults = (ClassificationInferenceResults)response.getInferenceResults().get(0);
        assertThat(classificationInferenceResults.getTopClasses(), hasSize(1));
        assertThat(classificationInferenceResults.getTopClasses().get(0).getClassification(), equalTo("to_be"));
    }

    public void testInferMissingModel() {
        String model = "test-infer-missing-model";
        InferModelAction.Request request = new InferModelAction.Request(model, Collections.emptyList(), new RegressionConfig());
        try {
            client().execute(InferModelAction.INSTANCE, request).actionGet();
        } catch (ElasticsearchException ex) {
            assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, model)));
        }
    }

    private static TrainedModelConfig.Builder buildTrainedModelConfigBuilder(String modelId) {
        return TrainedModelConfig.builder()
            .setCreatedBy("ml_test")
            .setDefinition(TrainedModelDefinitionTests.createRandomBuilder(modelId))
            .setDescription("trained model config for test")
            .setModelId(modelId);
    }

    @Override
    public NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

}
