/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.ingest;

import org.elasticsearch.client.Client;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
        String targetField = "classification_value";
        InferenceProcessor inferenceProcessor = new InferenceProcessor(client,
            auditor,
            "my_processor",
            targetField,
            "classification_model",
            ClassificationConfig.EMPTY_PARAMS,
            Collections.emptyMap(),
            "ml.my_processor",
            true,
            null);

        Map<String, Object> source = new HashMap<>();
        Map<String, Object> ingestMetadata = new HashMap<>();
        IngestDocument document = new IngestDocument(source, ingestMetadata);

        InternalInferModelAction.Response response = new InternalInferModelAction.Response(
            Collections.singletonList(new ClassificationInferenceResults(1.0, "foo", null)),
            true);
        inferenceProcessor.mutateDocument(response, document);

        assertThat(document.getFieldValue(targetField, String.class), equalTo("foo"));
        assertThat(document.getFieldValue("ml", Map.class),
            equalTo(Collections.singletonMap("my_processor", Collections.singletonMap("model_id", "classification_model"))));
    }

    @SuppressWarnings("unchecked")
    public void testMutateDocumentClassificationTopNClasses() {
        String targetField = "classification_value_probabilities";
        InferenceProcessor inferenceProcessor = new InferenceProcessor(client,
            auditor,
            "my_processor",
            targetField,
            "classification_model",
            new ClassificationConfig(2, null),
            Collections.emptyMap(),
            "ml.my_processor",
            true,
            null);

        Map<String, Object> source = new HashMap<>();
        Map<String, Object> ingestMetadata = new HashMap<>();
        IngestDocument document = new IngestDocument(source, ingestMetadata);

        List<ClassificationInferenceResults.TopClassEntry> classes = new ArrayList<>(2);
        classes.add(new ClassificationInferenceResults.TopClassEntry("foo", 0.6));
        classes.add(new ClassificationInferenceResults.TopClassEntry("bar", 0.4));

        InternalInferModelAction.Response response = new InternalInferModelAction.Response(
            Collections.singletonList(new ClassificationInferenceResults(1.0, "foo", classes)),
            true);
        inferenceProcessor.mutateDocument(response, document);

        assertThat((List<Map<?,?>>)document.getFieldValue(ClassificationConfig.DEFAULT_TOP_CLASSES_RESULT_FIELD, List.class),
            contains(classes.stream().map(ClassificationInferenceResults.TopClassEntry::asValueMap).toArray(Map[]::new)));
        assertThat(document.getFieldValue("ml", Map.class),
            equalTo(Collections.singletonMap("my_processor", Collections.singletonMap("model_id", "classification_model"))));
        assertThat(document.getFieldValue(targetField, String.class), equalTo("foo"));
    }

    @SuppressWarnings("unchecked")
    public void testMutateDocumentClassificationTopNClassesWithSpecificField() {
        String targetField = "classification_value_probabilities";
        InferenceProcessor inferenceProcessor = new InferenceProcessor(client,
            auditor,
            "my_processor",
            targetField,
            "classification_model",
            new ClassificationConfig(2, "my_top_classes"),
            Collections.emptyMap(),
            "ml.my_processor",
            true);

        Map<String, Object> source = new HashMap<>();
        Map<String, Object> ingestMetadata = new HashMap<>();
        IngestDocument document = new IngestDocument(source, ingestMetadata);

        List<ClassificationInferenceResults.TopClassEntry> classes = new ArrayList<>(2);
        classes.add(new ClassificationInferenceResults.TopClassEntry("foo", 0.6));
        classes.add(new ClassificationInferenceResults.TopClassEntry("bar", 0.4));

        InternalInferModelAction.Response response = new InternalInferModelAction.Response(
            Collections.singletonList(new ClassificationInferenceResults(1.0, "foo", classes)),
            true);
        inferenceProcessor.mutateDocument(response, document);

        assertThat((List<Map<?,?>>)document.getFieldValue("my_top_classes", List.class),
            contains(classes.stream().map(ClassificationInferenceResults.TopClassEntry::asValueMap).toArray(Map[]::new)));
        assertThat(document.getFieldValue("ml", Map.class),
            equalTo(Collections.singletonMap("my_processor", Collections.singletonMap("model_id", "classification_model"))));
        assertThat(document.getFieldValue(targetField, String.class), equalTo("foo"));
    }

    public void testMutateDocumentRegression() {
        String targetField = "regression_value";
        InferenceProcessor inferenceProcessor = new InferenceProcessor(client,
            auditor,
            "my_processor",
            targetField,
            "regression_model",
            new RegressionConfig(),
            Collections.emptyMap(),
            "ml.my_processor",
            true,
            null);

        Map<String, Object> source = new HashMap<>();
        Map<String, Object> ingestMetadata = new HashMap<>();
        IngestDocument document = new IngestDocument(source, ingestMetadata);

        InternalInferModelAction.Response response = new InternalInferModelAction.Response(
            Collections.singletonList(new RegressionInferenceResults(0.7)), true);
        inferenceProcessor.mutateDocument(response, document);

        assertThat(document.getFieldValue(targetField, Double.class), equalTo(0.7));
        assertThat(document.getFieldValue("ml", Map.class),
            equalTo(Collections.singletonMap("my_processor", Collections.singletonMap("model_id", "regression_model"))));
    }

    public void testMutateDocumentNoModelMetaData() {
        String targetField = "regression_value";
        InferenceProcessor inferenceProcessor = new InferenceProcessor(client,
            auditor,
            "my_processor",
            targetField,
            "regression_model",
            new RegressionConfig(),
            Collections.emptyMap(),
            "ml.my_processor",
            false,
            null);

        Map<String, Object> source = new HashMap<>();
        Map<String, Object> ingestMetadata = new HashMap<>();
        IngestDocument document = new IngestDocument(source, ingestMetadata);

        InternalInferModelAction.Response response = new InternalInferModelAction.Response(
            Collections.singletonList(new RegressionInferenceResults(0.7)), true);
        inferenceProcessor.mutateDocument(response, document);

        assertThat(document.getFieldValue(targetField, Double.class), equalTo(0.7));
        assertThat(document.hasField("ml"), is(false));
    }

    public void testMutateDocumentModelMetaDataExistingField() {
        String targetField = "regression_value";
        InferenceProcessor inferenceProcessor = new InferenceProcessor(client,
            auditor,
            "my_processor",
            targetField,
            "regression_model",
            new RegressionConfig(),
            Collections.emptyMap(),
            "ml.my_processor",
            true,
            null);

        //cannot use singleton map as attempting to mutate later
        Map<String, Object> ml = new HashMap<>(){{
            put("regression_prediction", 0.55);
        }};
        Map<String, Object> source = new HashMap<>(){{
            put("ml", ml);
        }};
        Map<String, Object> ingestMetadata = new HashMap<>();
        IngestDocument document = new IngestDocument(source, ingestMetadata);

        InternalInferModelAction.Response response = new InternalInferModelAction.Response(
            Collections.singletonList(new RegressionInferenceResults(0.7)), true);
        inferenceProcessor.mutateDocument(response, document);

        assertThat(document.getFieldValue(targetField, Double.class), equalTo(0.7));
        assertThat(document.getFieldValue("ml", Map.class),
            equalTo(new HashMap<>(){{
                put("my_processor", Collections.singletonMap("model_id", "regression_model"));
                put("regression_prediction", 0.55);
            }}));
    }

    public void testGenerateRequestWithEmptyMapping() {
        String modelId = "model";
        Integer topNClasses = randomBoolean() ? null : randomIntBetween(1, 10);

        InferenceProcessor processor = new InferenceProcessor(client,
            auditor,
            "my_processor",
            "my_field",
            modelId,
            new ClassificationConfig(topNClasses, null),
            Collections.emptyMap(),
            "ml.my_processor",
            false,
            null);

        Map<String, Object> source = new HashMap<>(){{
            put("value1", 1);
            put("value2", 4);
            put("categorical", "foo");
        }};
        Map<String, Object> ingestMetadata = new HashMap<>();
        IngestDocument document = new IngestDocument(source, ingestMetadata);

        assertThat(processor.buildRequest(document).getObjectsToInfer().get(0), equalTo(source));
    }

    public void testGenerateWithMapping() {
        String modelId = "model";
        Integer topNClasses = randomBoolean() ? null : randomIntBetween(1, 10);

        Map<String, String> fieldMapping = new HashMap<>(3) {{
            put("value1", "new_value1");
            put("value2", "new_value2");
            put("categorical", "new_categorical");
        }};

        InferenceProcessor processor = new InferenceProcessor(client,
            auditor,
            "my_processor",
            "my_field",
            modelId,
            new ClassificationConfig(topNClasses, null),
            fieldMapping,
            "ml.my_processor",
            false,
            null);

        Map<String, Object> source = new HashMap<>(3){{
            put("value1", 1);
            put("categorical", "foo");
            put("un_touched", "bar");
        }};
        Map<String, Object> ingestMetadata = new HashMap<>();
        IngestDocument document = new IngestDocument(source, ingestMetadata);

        Map<String, Object> expectedMap = new HashMap<>(2) {{
            put("new_value1", 1);
            put("new_categorical", "foo");
            put("un_touched", "bar");
        }};
        assertThat(processor.buildRequest(document).getObjectsToInfer().get(0), equalTo(expectedMap));
    }

    public void testHandleResponseLicenseChanged() {
        String targetField = "regression_value";
        InferenceProcessor inferenceProcessor = new InferenceProcessor(client,
            auditor,
            "my_processor",
            targetField,
            "regression_model",
            new RegressionConfig(),
            Collections.emptyMap(),
            "ml.my_processor",
            true,
            null);

        Map<String, Object> source = new HashMap<>();
        Map<String, Object> ingestMetadata = new HashMap<>();
        IngestDocument document = new IngestDocument(source, ingestMetadata);

        assertThat(inferenceProcessor.buildRequest(document).isPreviouslyLicensed(), is(false));

        InternalInferModelAction.Response response = new InternalInferModelAction.Response(
            Collections.singletonList(new RegressionInferenceResults(0.7)), true);
        inferenceProcessor.handleResponse(response, document, (doc, ex) -> {
            assertThat(doc, is(not(nullValue())));
            assertThat(ex, is(nullValue()));
        });

        assertThat(inferenceProcessor.buildRequest(document).isPreviouslyLicensed(), is(true));

        response = new InternalInferModelAction.Response(
            Collections.singletonList(new RegressionInferenceResults(0.7)), false);

        inferenceProcessor.handleResponse(response, document, (doc, ex) -> {
            assertThat(doc, is(not(nullValue())));
            assertThat(ex, is(nullValue()));
        });

        assertThat(inferenceProcessor.buildRequest(document).isPreviouslyLicensed(), is(true));

        inferenceProcessor.handleResponse(response, document, (doc, ex) -> {
            assertThat(doc, is(not(nullValue())));
            assertThat(ex, is(nullValue()));
        });

        verify(auditor, times(1)).warning(eq("regression_model"), any(String.class));
    }

    public void testMutateDocumentWithWarningResultAndNoWarningField() {
        String targetField = "regression_value";
        InferenceProcessor inferenceProcessor = new InferenceProcessor(client,
            auditor,
            "my_processor",
            targetField,
            "regression_model",
            new RegressionConfig(),
            Collections.emptyMap(),
            "ml.my_processor",
            false,
            null);

        Map<String, Object> source = new HashMap<>();
        Map<String, Object> ingestMetadata = new HashMap<>();
        IngestDocument document = new IngestDocument(source, ingestMetadata);

        InternalInferModelAction.Response response = new InternalInferModelAction.Response(
            Collections.singletonList(new WarningInferenceResults("something broke")), true);
        inferenceProcessor.mutateDocument(response, document);

        assertThat(document.hasField(targetField), is(false));
        assertThat(document.hasField("ml"), is(false));
    }

    public void testMutateDocumentWithWarningResultAndWarningField() {
        String targetField = "regression_value";
        InferenceProcessor inferenceProcessor = new InferenceProcessor(client,
            auditor,
            "my_processor",
            targetField,
            "regression_model",
            new RegressionConfig(),
            Collections.emptyMap(),
            "ml.my_processor",
            false,
            "ml.warning");

        Map<String, Object> source = new HashMap<>();
        Map<String, Object> ingestMetadata = new HashMap<>();
        IngestDocument document = new IngestDocument(source, ingestMetadata);

        InternalInferModelAction.Response response = new InternalInferModelAction.Response(
            Collections.singletonList(new WarningInferenceResults("something broke")), true);
        inferenceProcessor.mutateDocument(response, document);

        assertThat(document.hasField(targetField), is(false));
        assertThat(document.hasField("ml.warning"), is(true));
        assertThat(document.hasField("ml.my_processor"), is(false));
    }
}
