/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.ingest;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationFeatureImportance;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionFeatureImportance;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PredictionFieldType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdate;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class InferenceProcessorTests extends ESTestCase {

    private Client client;
    private InferenceAuditor auditor;

    @Before
    public void setUpVariables() {
        client = mock(Client.class);
        auditor = mock(InferenceAuditor.class);
    }

    public void testMutateDocumentWithClassification() {
        String targetField = "ml.my_processor";
        InferenceProcessor inferenceProcessor = InferenceProcessor.fromTargetFieldConfiguration(
            client,
            auditor,
            "my_processor",
            null,
            targetField,
            "classification_model",
            ClassificationConfigUpdate.EMPTY_PARAMS,
            Collections.emptyMap()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();

        InferModelAction.Response response = new InferModelAction.Response(
            Collections.singletonList(
                new ClassificationInferenceResults(1.0, "foo", null, Collections.emptyList(), ClassificationConfig.EMPTY_PARAMS, 1.0, 1.0)
            ),
            null,
            true
        );
        inferenceProcessor.mutateDocument(response, document);

        assertThat(
            document.getFieldValue(targetField + "." + ClassificationConfig.EMPTY_PARAMS.getResultsField(), String.class),
            equalTo("foo")
        );
        assertThat(document.getFieldValue("ml.my_processor.model_id", String.class), equalTo("classification_model"));
    }

    @SuppressWarnings("unchecked")
    public void testMutateDocumentClassificationTopNClasses() {
        ClassificationConfigUpdate classificationConfigUpdate = new ClassificationConfigUpdate(2, null, null, null, null);
        ClassificationConfig classificationConfig = new ClassificationConfig(2, null, null, null, PredictionFieldType.STRING);
        InferenceProcessor inferenceProcessor = InferenceProcessor.fromTargetFieldConfiguration(
            client,
            auditor,
            "my_processor",
            null,
            "ml.my_processor",
            "classification_model",
            classificationConfigUpdate,
            Collections.emptyMap()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();

        List<TopClassEntry> classes = new ArrayList<>(2);
        classes.add(new TopClassEntry("foo", 0.6, 0.6));
        classes.add(new TopClassEntry("bar", 0.4, 0.4));

        InferModelAction.Response response = new InferModelAction.Response(
            Collections.singletonList(
                new ClassificationInferenceResults(1.0, "foo", classes, Collections.emptyList(), classificationConfig, 0.6, 0.6)
            ),
            null,
            true
        );
        inferenceProcessor.mutateDocument(response, document);

        assertThat(
            (List<Map<?, ?>>) document.getFieldValue("ml.my_processor.top_classes", List.class),
            contains(classes.stream().map(TopClassEntry::asValueMap).toArray(Map[]::new))
        );
        assertThat(document.getFieldValue("ml.my_processor.model_id", String.class), equalTo("classification_model"));
        assertThat(document.getFieldValue("ml.my_processor.predicted_value", String.class), equalTo("foo"));
    }

    public void testMutateDocumentClassificationFeatureInfluence() {
        ClassificationConfig classificationConfig = new ClassificationConfig(2, null, null, 2, PredictionFieldType.STRING);
        ClassificationConfigUpdate classificationConfigUpdate = new ClassificationConfigUpdate(2, null, null, 2, null);
        InferenceProcessor inferenceProcessor = InferenceProcessor.fromTargetFieldConfiguration(
            client,
            auditor,
            "my_processor",
            null,
            "ml.my_processor",
            "classification_model",
            classificationConfigUpdate,
            Collections.emptyMap()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();

        List<TopClassEntry> classes = new ArrayList<>(2);
        classes.add(new TopClassEntry("foo", 0.6, 0.6));
        classes.add(new TopClassEntry("bar", 0.4, 0.4));

        List<ClassificationFeatureImportance> featureInfluence = new ArrayList<>();
        featureInfluence.add(
            new ClassificationFeatureImportance(
                "feature_1",
                Collections.singletonList(new ClassificationFeatureImportance.ClassImportance("class_a", 1.13))
            )
        );
        featureInfluence.add(
            new ClassificationFeatureImportance(
                "feature_2",
                Collections.singletonList(new ClassificationFeatureImportance.ClassImportance("class_b", -42.0))
            )
        );

        InferModelAction.Response response = new InferModelAction.Response(
            Collections.singletonList(
                new ClassificationInferenceResults(1.0, "foo", classes, featureInfluence, classificationConfig, 0.6, 0.6)
            ),
            null,
            true
        );
        inferenceProcessor.mutateDocument(response, document);

        assertThat(document.getFieldValue("ml.my_processor.model_id", String.class), equalTo("classification_model"));
        assertThat(document.getFieldValue("ml.my_processor.predicted_value", String.class), equalTo("foo"));
        assertThat(document.getFieldValue("ml.my_processor.feature_importance.0.feature_name", String.class), equalTo("feature_2"));
        assertThat(document.getFieldValue("ml.my_processor.feature_importance.0.classes.0.class_name", String.class), equalTo("class_b"));
        assertThat(document.getFieldValue("ml.my_processor.feature_importance.0.classes.0.importance", Double.class), equalTo(-42.0));
        assertThat(document.getFieldValue("ml.my_processor.feature_importance.1.feature_name", String.class), equalTo("feature_1"));
        assertThat(document.getFieldValue("ml.my_processor.feature_importance.1.classes.0.class_name", String.class), equalTo("class_a"));
        assertThat(document.getFieldValue("ml.my_processor.feature_importance.1.classes.0.importance", Double.class), equalTo(1.13));
    }

    @SuppressWarnings("unchecked")
    public void testMutateDocumentClassificationTopNClassesWithSpecificField() {
        ClassificationConfig classificationConfig = new ClassificationConfig(2, "result", "tops", null, PredictionFieldType.STRING);
        ClassificationConfigUpdate classificationConfigUpdate = new ClassificationConfigUpdate(2, "result", "tops", null, null);
        InferenceProcessor inferenceProcessor = InferenceProcessor.fromTargetFieldConfiguration(
            client,
            auditor,
            "my_processor",
            null,
            "ml.my_processor",
            "classification_model",
            classificationConfigUpdate,
            Collections.emptyMap()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();

        List<TopClassEntry> classes = new ArrayList<>(2);
        classes.add(new TopClassEntry("foo", 0.6, 0.6));
        classes.add(new TopClassEntry("bar", 0.4, 0.4));

        InferModelAction.Response response = new InferModelAction.Response(
            Collections.singletonList(
                new ClassificationInferenceResults(1.0, "foo", classes, Collections.emptyList(), classificationConfig, 0.6, 0.6)
            ),
            null,
            true
        );
        inferenceProcessor.mutateDocument(response, document);

        assertThat(
            (List<Map<?, ?>>) document.getFieldValue("ml.my_processor.tops", List.class),
            contains(classes.stream().map(TopClassEntry::asValueMap).toArray(Map[]::new))
        );
        assertThat(document.getFieldValue("ml.my_processor.model_id", String.class), equalTo("classification_model"));
        assertThat(document.getFieldValue("ml.my_processor.result", String.class), equalTo("foo"));
    }

    public void testMutateDocumentRegression() {
        RegressionConfig regressionConfig = new RegressionConfig("foo");
        RegressionConfigUpdate regressionConfigUpdate = new RegressionConfigUpdate("foo", null);
        InferenceProcessor inferenceProcessor = InferenceProcessor.fromTargetFieldConfiguration(
            client,
            auditor,
            "my_processor",
            null,
            "ml.my_processor",
            "regression_model",
            regressionConfigUpdate,
            Collections.emptyMap()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();

        InferModelAction.Response response = new InferModelAction.Response(
            Collections.singletonList(new RegressionInferenceResults(0.7, regressionConfig)),
            null,
            true
        );
        inferenceProcessor.mutateDocument(response, document);

        assertThat(document.getFieldValue("ml.my_processor.foo", Double.class), equalTo(0.7));
        assertThat(document.getFieldValue("ml.my_processor.model_id", String.class), equalTo("regression_model"));
    }

    public void testMutateDocumentRegressionWithTopFeatures() {
        RegressionConfig regressionConfig = new RegressionConfig("foo", 2);
        RegressionConfigUpdate regressionConfigUpdate = new RegressionConfigUpdate("foo", 2);
        InferenceProcessor inferenceProcessor = InferenceProcessor.fromTargetFieldConfiguration(
            client,
            auditor,
            "my_processor",
            null,
            "ml.my_processor",
            "regression_model",
            regressionConfigUpdate,
            Collections.emptyMap()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();

        List<RegressionFeatureImportance> featureInfluence = new ArrayList<>();
        featureInfluence.add(new RegressionFeatureImportance("feature_1", 1.13));
        featureInfluence.add(new RegressionFeatureImportance("feature_2", -42.0));

        InferModelAction.Response response = new InferModelAction.Response(
            Collections.singletonList(new RegressionInferenceResults(0.7, regressionConfig, featureInfluence)),
            null,
            true
        );
        inferenceProcessor.mutateDocument(response, document);

        assertThat(document.getFieldValue("ml.my_processor.foo", Double.class), equalTo(0.7));
        assertThat(document.getFieldValue("ml.my_processor.model_id", String.class), equalTo("regression_model"));
        assertThat(document.getFieldValue("ml.my_processor.feature_importance.0.importance", Double.class), equalTo(-42.0));
        assertThat(document.getFieldValue("ml.my_processor.feature_importance.0.feature_name", String.class), equalTo("feature_2"));
        assertThat(document.getFieldValue("ml.my_processor.feature_importance.1.importance", Double.class), equalTo(1.13));
        assertThat(document.getFieldValue("ml.my_processor.feature_importance.1.feature_name", String.class), equalTo("feature_1"));
    }

    public void testGenerateRequestWithEmptyMapping() {
        String modelId = "model";
        Integer topNClasses = randomBoolean() ? null : randomIntBetween(1, 10);

        InferenceProcessor processor = InferenceProcessor.fromTargetFieldConfiguration(
            client,
            auditor,
            "my_processor",
            null,
            "my_field",
            modelId,
            new ClassificationConfigUpdate(topNClasses, null, null, null, null),
            Collections.emptyMap()
        );

        Map<String, Object> source = new HashMap<>() {
            {
                put("value1", 1);
                put("value2", 4);
                put("categorical", "foo");
            }
        };
        IngestDocument document = TestIngestDocument.ofIngestWithNullableVersion(source, new HashMap<>());

        var request = processor.buildRequest(document);
        assertThat(request.getObjectsToInfer().get(0), equalTo(source));
        assertEquals(InferModelAction.Request.DEFAULT_TIMEOUT_FOR_INGEST, request.getInferenceTimeout());
        assertEquals(TrainedModelPrefixStrings.PrefixType.INGEST, request.getPrefixType());

        Map<String, Object> ingestMetadata = Collections.singletonMap("_value", 3);
        document = TestIngestDocument.ofIngestWithNullableVersion(source, ingestMetadata);

        Map<String, Object> expected = new HashMap<>(source);
        expected.put("_ingest", ingestMetadata);

        request = processor.buildRequest(document);
        assertThat(request.getObjectsToInfer().get(0), equalTo(expected));
        assertEquals(InferModelAction.Request.DEFAULT_TIMEOUT_FOR_INGEST, request.getInferenceTimeout());
        assertEquals(TrainedModelPrefixStrings.PrefixType.INGEST, request.getPrefixType());
    }

    public void testGenerateWithMapping() {
        String modelId = "model";
        Integer topNClasses = randomBoolean() ? null : randomIntBetween(1, 10);

        Map<String, String> fieldMapping = Maps.newMapWithExpectedSize(4);
        fieldMapping.put("value1", "new_value1");
        fieldMapping.put("value2", "new_value2");
        fieldMapping.put("categorical", "new_categorical");
        fieldMapping.put("_ingest._value", "metafield");

        InferenceProcessor processor = InferenceProcessor.fromTargetFieldConfiguration(
            client,
            auditor,
            "my_processor",
            null,
            "my_field",
            modelId,
            new ClassificationConfigUpdate(topNClasses, null, null, null, null),
            fieldMapping
        );

        Map<String, Object> source = Maps.newMapWithExpectedSize(3);
        source.put("value1", 1);
        source.put("categorical", "foo");
        source.put("un_touched", "bar");
        IngestDocument document = TestIngestDocument.withNullableVersion(source);

        Map<String, Object> expectedMap = Maps.newMapWithExpectedSize(5);
        expectedMap.put("new_value1", 1);
        expectedMap.put("value1", 1);
        expectedMap.put("categorical", "foo");
        expectedMap.put("new_categorical", "foo");
        expectedMap.put("un_touched", "bar");
        var request = processor.buildRequest(document);
        assertThat(request.getObjectsToInfer().get(0), equalTo(expectedMap));
        assertEquals(InferModelAction.Request.DEFAULT_TIMEOUT_FOR_INGEST, request.getInferenceTimeout());
        assertEquals(TrainedModelPrefixStrings.PrefixType.INGEST, request.getPrefixType());

        Map<String, Object> ingestMetadata = Collections.singletonMap("_value", "baz");
        document = TestIngestDocument.ofIngestWithNullableVersion(source, ingestMetadata);
        expectedMap = new HashMap<>(expectedMap);
        expectedMap.put("metafield", "baz");
        expectedMap.put("_ingest", ingestMetadata);
        request = processor.buildRequest(document);
        assertThat(request.getObjectsToInfer().get(0), equalTo(expectedMap));
        assertEquals(InferModelAction.Request.DEFAULT_TIMEOUT_FOR_INGEST, request.getInferenceTimeout());
        assertEquals(TrainedModelPrefixStrings.PrefixType.INGEST, request.getPrefixType());
    }

    public void testGenerateWithMappingNestedFields() {
        String modelId = "model";
        Integer topNClasses = randomBoolean() ? null : randomIntBetween(1, 10);

        Map<String, String> fieldMapping = Maps.newMapWithExpectedSize(3);
        fieldMapping.put("value1.foo", "new_value1");
        fieldMapping.put("value2", "new_value2");
        fieldMapping.put("categorical.bar", "new_categorical");

        InferenceProcessor processor = InferenceProcessor.fromTargetFieldConfiguration(
            client,
            auditor,
            "my_processor",
            null,
            "my_field",
            modelId,
            new ClassificationConfigUpdate(topNClasses, null, null, null, null),
            fieldMapping
        );

        Map<String, Object> source = Maps.newMapWithExpectedSize(3);
        source.put("value1", Collections.singletonMap("foo", 1));
        source.put("categorical.bar", "foo");
        source.put("un_touched", "bar");
        IngestDocument document = TestIngestDocument.withNullableVersion(source);

        Map<String, Object> expectedMap = Maps.newMapWithExpectedSize(5);
        expectedMap.put("new_value1", 1);
        expectedMap.put("value1", Collections.singletonMap("foo", 1));
        expectedMap.put("categorical.bar", "foo");
        expectedMap.put("new_categorical", "foo");
        expectedMap.put("un_touched", "bar");
        assertThat(processor.buildRequest(document).getObjectsToInfer().get(0), equalTo(expectedMap));
    }

    public void testHandleResponseLicenseChanged() {
        String targetField = "regression_value";
        InferenceProcessor inferenceProcessor = InferenceProcessor.fromTargetFieldConfiguration(
            client,
            auditor,
            "my_processor",
            null,
            targetField,
            "regression_model",
            RegressionConfigUpdate.EMPTY_PARAMS,
            Collections.emptyMap()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();

        assertThat(inferenceProcessor.buildRequest(document).getPreviouslyLicensed(), is(false));

        InferModelAction.Response response = new InferModelAction.Response(
            Collections.singletonList(new RegressionInferenceResults(0.7, RegressionConfig.EMPTY_PARAMS)),
            null,
            true
        );
        inferenceProcessor.handleResponse(response, document, (doc, ex) -> {
            assertThat(doc, is(not(nullValue())));
            assertThat(ex, is(nullValue()));
        });

        assertThat(inferenceProcessor.buildRequest(document).getPreviouslyLicensed(), is(true));

        response = new InferModelAction.Response(
            Collections.singletonList(new RegressionInferenceResults(0.7, RegressionConfig.EMPTY_PARAMS)),
            null,
            false
        );

        inferenceProcessor.handleResponse(response, document, (doc, ex) -> {
            assertThat(doc, is(not(nullValue())));
            assertThat(ex, is(nullValue()));
        });

        assertThat(inferenceProcessor.buildRequest(document).getPreviouslyLicensed(), is(true));

        inferenceProcessor.handleResponse(response, document, (doc, ex) -> {
            assertThat(doc, is(not(nullValue())));
            assertThat(ex, is(nullValue()));
        });

        verify(auditor, times(1)).warning(eq("regression_model"), any(String.class));
    }

    public void testMutateDocumentWithWarningResult() {
        String targetField = "regression_value";
        InferenceProcessor inferenceProcessor = InferenceProcessor.fromTargetFieldConfiguration(
            client,
            auditor,
            "my_processor",
            null,
            "ml",
            "regression_model",
            RegressionConfigUpdate.EMPTY_PARAMS,
            Collections.emptyMap()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();

        InferModelAction.Response response = new InferModelAction.Response(
            Collections.singletonList(new WarningInferenceResults("something broke")),
            null,
            true
        );
        inferenceProcessor.mutateDocument(response, document);

        assertThat(document.hasField(targetField), is(false));
        assertThat(document.hasField("ml.warning"), is(true));
        assertThat(document.hasField("ml.my_processor"), is(false));
    }

    public void testMutateDocumentWithModelIdResult() {
        String modelAlias = "special_model";
        String modelId = "regression-123";
        InferenceProcessor inferenceProcessor = InferenceProcessor.fromTargetFieldConfiguration(
            client,
            auditor,
            "my_processor",
            null,
            "ml.my_processor",
            modelAlias,
            new RegressionConfigUpdate("foo", null),
            Collections.emptyMap()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();

        InferModelAction.Response response = new InferModelAction.Response(
            Collections.singletonList(new RegressionInferenceResults(0.7, new RegressionConfig("foo"))),
            modelId,
            true
        );
        inferenceProcessor.mutateDocument(response, document);

        assertThat(document.getFieldValue("ml.my_processor.foo", Double.class), equalTo(0.7));
        assertThat(document.getFieldValue("ml.my_processor.model_id", String.class), equalTo(modelId));
    }

    public void testMutateDocumentWithInputFields() {
        String modelId = "regression-123";
        List<InferenceProcessor.Factory.InputConfig> inputs = new ArrayList<>();
        inputs.add(new InferenceProcessor.Factory.InputConfig("body", null, "body_result", Map.of()));
        inputs.add(new InferenceProcessor.Factory.InputConfig("content", null, "content_result", Map.of()));

        InferenceProcessor inferenceProcessor = InferenceProcessor.fromInputFieldConfiguration(
            client,
            auditor,
            "my_processor_tag",
            "description",
            modelId,
            new RegressionConfigUpdate("foo", null),
            inputs,
            randomBoolean()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();

        InferModelAction.Response response = new InferModelAction.Response(
            List.of(new RegressionInferenceResults(0.7, "ignore"), new RegressionInferenceResults(1.0, "ignore")),
            modelId,
            true
        );
        inferenceProcessor.mutateDocument(response, document);

        assertThat(document.getFieldValue("body_result", Double.class), equalTo(0.7));
        assertThat(document.getFieldValue("content_result", Double.class), equalTo(1.0));
    }

    public void testMutateDocumentWithInputFieldsNested() {
        String modelId = "elser";
        List<InferenceProcessor.Factory.InputConfig> inputs = new ArrayList<>();
        inputs.add(new InferenceProcessor.Factory.InputConfig("body", "ml.results", "body_tokens", Map.of()));
        inputs.add(new InferenceProcessor.Factory.InputConfig("content", "ml.results", "content_tokens", Map.of()));

        InferenceProcessor inferenceProcessor = InferenceProcessor.fromInputFieldConfiguration(
            client,
            auditor,
            "my_processor_tag",
            "description",
            modelId,
            new RegressionConfigUpdate("foo", null),
            inputs,
            randomBoolean()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();

        var teResult1 = TextExpansionResultsTests.createRandomResults();
        var teResult2 = TextExpansionResultsTests.createRandomResults();
        InferModelAction.Response response = new InferModelAction.Response(List.of(teResult1, teResult2), modelId, true);
        inferenceProcessor.mutateDocument(response, document);

        assertEquals(modelId, document.getFieldValue("ml.results.model_id", String.class));

        var bodyTokens = document.getFieldValue("ml.results.body_tokens", HashMap.class);
        assertEquals(teResult1.getWeightedTokens().size(), bodyTokens.entrySet().size());
        if (teResult1.getWeightedTokens().isEmpty() == false) {
            assertEquals(
                (float) bodyTokens.get(teResult1.getWeightedTokens().get(0).token()),
                teResult1.getWeightedTokens().get(0).weight(),
                0.001
            );
        }
        var contentTokens = document.getFieldValue("ml.results.content_tokens", HashMap.class);
        assertEquals(teResult2.getWeightedTokens().size(), contentTokens.entrySet().size());
        if (teResult2.getWeightedTokens().isEmpty() == false) {
            assertEquals(
                (float) contentTokens.get(teResult2.getWeightedTokens().get(0).token()),
                teResult2.getWeightedTokens().get(0).weight(),
                0.001
            );
        }
    }

    public void testBuildRequestWithInputFields() {
        String modelId = "elser";
        List<InferenceProcessor.Factory.InputConfig> inputs = new ArrayList<>();
        inputs.add(new InferenceProcessor.Factory.InputConfig("body.text", "ml.results", "body_tokens", Map.of()));
        inputs.add(new InferenceProcessor.Factory.InputConfig("title.text", "ml.results", "title_tokens", Map.of()));

        InferenceProcessor inferenceProcessor = InferenceProcessor.fromInputFieldConfiguration(
            client,
            auditor,
            "my_processor_tag",
            "description",
            modelId,
            new EmptyConfigUpdate(),
            inputs,
            randomBoolean()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();
        document.setFieldValue("body.text", "body_text");
        document.setFieldValue("title.text", "title_text");
        document.setFieldValue("unrelated", "text");

        var request = inferenceProcessor.buildRequest(document);
        assertNull(request.getObjectsToInfer());
        var requestInputs = request.getInputs();
        assertThat(requestInputs, contains("body_text", "title_text"));
        assertEquals(InferModelAction.Request.DEFAULT_TIMEOUT_FOR_INGEST, request.getInferenceTimeout());
        assertEquals(TrainedModelPrefixStrings.PrefixType.INGEST, request.getPrefixType());
    }

    public void testBuildRequestWithInputFields_WrongType() {
        String modelId = "elser";
        List<InferenceProcessor.Factory.InputConfig> inputs = new ArrayList<>();
        inputs.add(new InferenceProcessor.Factory.InputConfig("not_a_string", "ml.results", "tokens", Map.of()));

        InferenceProcessor inferenceProcessor = InferenceProcessor.fromInputFieldConfiguration(
            client,
            auditor,
            "my_processor_tag",
            "description",
            modelId,
            new EmptyConfigUpdate(),
            inputs,
            randomBoolean()
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();
        document.setFieldValue("not_a_string", Boolean.TRUE);
        document.setFieldValue("unrelated", "text");

        var e = expectThrows(IllegalArgumentException.class, () -> inferenceProcessor.buildRequest(document));
        assertThat(e.getMessage(), containsString("input field [not_a_string] cannot be processed because it is not a text field"));
    }

    public void testBuildRequestWithInputFields_MissingField() {
        String modelId = "elser";
        List<InferenceProcessor.Factory.InputConfig> inputs = new ArrayList<>();
        inputs.add(new InferenceProcessor.Factory.InputConfig("body.text", "ml.results", "body_tokens", Map.of()));
        inputs.add(new InferenceProcessor.Factory.InputConfig("title.text", "ml.results", "title_tokens", Map.of()));

        {
            InferenceProcessor inferenceProcessor = InferenceProcessor.fromInputFieldConfiguration(
                client,
                auditor,
                "my_processor_tag",
                "description",
                modelId,
                new EmptyConfigUpdate(),
                inputs,
                false
            );

            IngestDocument document = TestIngestDocument.emptyIngestDocument();
            document.setFieldValue("body.text", "body_text");
            document.setFieldValue("unrelated", "text");

            var e = expectThrows(IllegalArgumentException.class, () -> inferenceProcessor.buildRequest(document));
            assertThat(e.getMessage(), containsString("field [title] not present as part of path [title.text]"));
        }

        // same test with ignore_missing == true
        {
            InferenceProcessor inferenceProcessor = InferenceProcessor.fromInputFieldConfiguration(
                client,
                auditor,
                "my_processor_tag",
                "description",
                modelId,
                new EmptyConfigUpdate(),
                inputs,
                true
            );

            IngestDocument document = TestIngestDocument.emptyIngestDocument();
            document.setFieldValue("body.text", "body_text");
            document.setFieldValue("unrelated", 1.0);

            var request = inferenceProcessor.buildRequest(document);
            var requestInputs = request.getInputs();
            assertThat(requestInputs, contains("body_text", ""));
        }
    }
}
