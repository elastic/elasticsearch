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
import org.elasticsearch.license.License;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction;
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
import static org.hamcrest.CoreMatchers.instanceOf;
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
            .setInput(new TrainedModelInput(Arrays.asList("field.foo", "field.bar", "other.categorical")))
            .setParsedDefinition(new TrainedModelDefinition.Builder()
                .setPreProcessors(Arrays.asList(new OneHotEncoding("other.categorical", oneHotEncoding)))
                .setTrainedModel(buildClassification(true)))
            .setVersion(Version.CURRENT)
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setCreateTime(Instant.now())
            .setEstimatedOperations(0)
            .setEstimatedHeapMemory(0)
            .build();
        TrainedModelConfig config2 = buildTrainedModelConfigBuilder(modelId1)
            .setInput(new TrainedModelInput(Arrays.asList("field.foo", "field.bar", "other.categorical")))
            .setParsedDefinition(new TrainedModelDefinition.Builder()
                .setPreProcessors(Arrays.asList(new OneHotEncoding("other.categorical", oneHotEncoding)))
                .setTrainedModel(buildRegression()))
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
            put("field", new HashMap<>(){{
                put("foo", 1.0);
                put("bar", 0.5);
            }});
            put("other", new HashMap<>(){{
                put("categorical", "dog");
            }});
        }});
        toInfer.add(new HashMap<>() {{
            put("field", new HashMap<>(){{
                put("foo", 0.9);
                put("bar", 1.5);
            }});
            put("other", new HashMap<>(){{
                put("categorical", "cat");
            }});
        }});

        List<Map<String, Object>> toInfer2 = new ArrayList<>();
        toInfer2.add(new HashMap<>() {{
            put("field", new HashMap<>(){{
                put("foo", 0.0);
                put("bar", 0.01);
            }});
            put("other", new HashMap<>(){{
                put("categorical", "dog");
            }});
        }});
        toInfer2.add(new HashMap<>() {{
            put("field", new HashMap<>(){{
                put("foo", 1.0);
                put("bar", 0.0);
            }});
            put("other", new HashMap<>(){{
                put("categorical", "cat");
            }});
        }});

        // Test regression
        InternalInferModelAction.Request request = new InternalInferModelAction.Request(modelId1,
            toInfer,
            RegressionConfig.EMPTY_PARAMS,
            true);
        InternalInferModelAction.Response response = client().execute(InternalInferModelAction.INSTANCE, request).actionGet();
        assertThat(response.getInferenceResults().stream().map(i -> ((SingleValueInferenceResults)i).value()).collect(Collectors.toList()),
            contains(1.3, 1.25));

        request = new InternalInferModelAction.Request(modelId1, toInfer2, RegressionConfig.EMPTY_PARAMS, true);
        response = client().execute(InternalInferModelAction.INSTANCE, request).actionGet();
        assertThat(response.getInferenceResults().stream().map(i -> ((SingleValueInferenceResults)i).value()).collect(Collectors.toList()),
            contains(1.65, 1.55));


        // Test classification
        request = new InternalInferModelAction.Request(modelId2, toInfer, ClassificationConfig.EMPTY_PARAMS, true);
        response = client().execute(InternalInferModelAction.INSTANCE, request).actionGet();
        assertThat(response.getInferenceResults()
                .stream()
                .map(i -> ((SingleValueInferenceResults)i).valueAsString())
                .collect(Collectors.toList()),
            contains("not_to_be", "to_be"));

        // Get top classes
        request = new InternalInferModelAction.Request(modelId2, toInfer, new ClassificationConfig(2, null, null), true);
        response = client().execute(InternalInferModelAction.INSTANCE, request).actionGet();

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
        request = new InternalInferModelAction.Request(modelId2, toInfer2, new ClassificationConfig(1, null, null), true);
        response = client().execute(InternalInferModelAction.INSTANCE, request).actionGet();

        classificationInferenceResults = (ClassificationInferenceResults)response.getInferenceResults().get(0);
        assertThat(classificationInferenceResults.getTopClasses(), hasSize(1));
        assertThat(classificationInferenceResults.getTopClasses().get(0).getClassification(), equalTo("to_be"));
    }

    public void testInferMissingModel() {
        String model = "test-infer-missing-model";
        InternalInferModelAction.Request request = new InternalInferModelAction.Request(
            model,
            Collections.emptyList(),
            RegressionConfig.EMPTY_PARAMS,
            true);
        try {
            client().execute(InternalInferModelAction.INSTANCE, request).actionGet();
        } catch (ElasticsearchException ex) {
            assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, model)));
        }
    }

    public void testInferMissingFields() throws Exception {
        String modelId = "test-load-models-regression-missing-fields";
        Map<String, String> oneHotEncoding = new HashMap<>();
        oneHotEncoding.put("cat", "animal_cat");
        oneHotEncoding.put("dog", "animal_dog");
        TrainedModelConfig config = buildTrainedModelConfigBuilder(modelId)
            .setInput(new TrainedModelInput(Arrays.asList("field1", "field2")))
            .setParsedDefinition(new TrainedModelDefinition.Builder()
                .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotEncoding)))
                .setTrainedModel(buildRegression()))
            .setVersion(Version.CURRENT)
            .setEstimatedOperations(0)
            .setEstimatedHeapMemory(0)
            .setCreateTime(Instant.now())
            .build();
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));


        List<Map<String, Object>> toInferMissingField = new ArrayList<>();
        toInferMissingField.add(new HashMap<>() {{
            put("foo", 1.0);
            put("bar", 0.5);
        }});

        InternalInferModelAction.Request request = new InternalInferModelAction.Request(
            modelId,
            toInferMissingField,
            RegressionConfig.EMPTY_PARAMS,
            true);
        try {
            InferenceResults result =
                client().execute(InternalInferModelAction.INSTANCE, request).actionGet().getInferenceResults().get(0);
            assertThat(result, is(instanceOf(WarningInferenceResults.class)));
            assertThat(((WarningInferenceResults)result).getWarning(),
                equalTo(Messages.getMessage(Messages.INFERENCE_WARNING_ALL_FIELDS_MISSING, modelId)));
        } catch (ElasticsearchException ex) {
            fail("Should not have thrown. Ex: " + ex.getMessage());
        }
    }

    private static TrainedModelConfig.Builder buildTrainedModelConfigBuilder(String modelId) {
        return TrainedModelConfig.builder()
            .setCreatedBy("ml_test")
            .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder())
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
