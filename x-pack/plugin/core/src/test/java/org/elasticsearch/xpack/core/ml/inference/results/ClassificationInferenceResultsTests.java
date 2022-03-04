/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PredictionFieldType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ml.inference.results.InferenceResults.writeResult;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ClassificationInferenceResultsTests extends InferenceResultsTestCase<ClassificationInferenceResults> {

    public static ClassificationInferenceResults createRandomResults() {
        ClassificationConfig config = ClassificationConfigTests.randomClassificationConfig();
        double value = randomDouble();
        if (config.getPredictionFieldType() == PredictionFieldType.BOOLEAN) {
            // value must be close to 0 or 1
            value = randomBoolean() ? 0.0 : 1.0;
        }

        return new ClassificationInferenceResults(
            value,
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean()
                ? null
                : Stream.generate(TopClassEntryTests::createRandomTopClassEntry)
                    .limit(randomIntBetween(0, 10))
                    .collect(Collectors.toList()),
            randomBoolean()
                ? null
                : Stream.generate(ClassificationFeatureImportanceTests::createRandomInstance)
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.toList()),
            config,
            randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, false),
            randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, false)
        );
    }

    @SuppressWarnings("unchecked")
    public void testWriteResultsWithTopClasses() {
        List<TopClassEntry> entries = Arrays.asList(
            new TopClassEntry("foo", 0.7, 0.7),
            new TopClassEntry("bar", 0.2, 0.2),
            new TopClassEntry("baz", 0.1, 0.1)
        );
        ClassificationInferenceResults result = new ClassificationInferenceResults(
            1.0,
            "foo",
            entries,
            Collections.emptyList(),
            new ClassificationConfig(3, "my_results", "bar", null, PredictionFieldType.STRING),
            0.7,
            0.7
        );
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        writeResult(result, document, "result_field", "test");

        List<?> list = document.getFieldValue("result_field.bar", List.class);
        assertThat(list.size(), equalTo(3));

        for (int i = 0; i < 3; i++) {
            Map<String, Object> map = (Map<String, Object>) list.get(i);
            assertThat(map, equalTo(entries.get(i).asValueMap()));
        }

        assertThat(document.getFieldValue("result_field.my_results", String.class), equalTo("foo"));
    }

    public void testWriteResultsWithImportance() {
        List<ClassificationFeatureImportance> importanceList = Stream.generate(ClassificationFeatureImportanceTests::createRandomInstance)
            .limit(5)
            .collect(Collectors.toList());
        ClassificationInferenceResults result = new ClassificationInferenceResults(
            0.0,
            "foo",
            Collections.emptyList(),
            importanceList,
            new ClassificationConfig(0, "predicted_value", "top_classes", 3, PredictionFieldType.STRING),
            1.0,
            1.0
        );
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        writeResult(result, document, "result_field", "test");

        assertThat(document.getFieldValue("result_field.predicted_value", String.class), equalTo("foo"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> writtenImportance = (List<Map<String, Object>>) document.getFieldValue(
            "result_field.feature_importance",
            List.class
        );
        assertThat(writtenImportance, hasSize(3));
        importanceList.sort((l, r) -> Double.compare(Math.abs(r.getTotalImportance()), Math.abs(l.getTotalImportance())));
        for (int i = 0; i < 3; i++) {
            Map<String, Object> objectMap = writtenImportance.get(i);
            ClassificationFeatureImportance importance = importanceList.get(i);
            assertThat(objectMap.get("feature_name"), equalTo(importance.getFeatureName()));
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> classImportances = (List<Map<String, Object>>) objectMap.get("classes");
            if (importance.getClassImportance() != null) {
                for (int j = 0; j < importance.getClassImportance().size(); j++) {
                    Map<String, Object> classMap = classImportances.get(j);
                    ClassificationFeatureImportance.ClassImportance classImportance = importance.getClassImportance().get(j);
                    assertThat(classMap.get("class_name"), equalTo(classImportance.getClassName()));
                    assertThat(classMap.get("importance"), equalTo(classImportance.getImportance()));
                }
            }
        }
    }

    @Override
    protected ClassificationInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected Writeable.Reader<ClassificationInferenceResults> instanceReader() {
        return ClassificationInferenceResults::new;
    }

    public void testToXContent() throws IOException {
        ClassificationConfig toStringConfig = new ClassificationConfig(1, null, null, null, PredictionFieldType.STRING);
        ClassificationInferenceResults result = new ClassificationInferenceResults(
            1.0,
            null,
            null,
            Collections.emptyList(),
            toStringConfig,
            1.0,
            1.0
        );
        String stringRep = Strings.toString(result);
        String expected = """
            {"predicted_value":"1.0","prediction_probability":1.0,"prediction_score":1.0}""";
        assertEquals(expected, stringRep);

        ClassificationConfig toDoubleConfig = new ClassificationConfig(1, null, null, null, PredictionFieldType.NUMBER);
        result = new ClassificationInferenceResults(1.0, null, null, Collections.emptyList(), toDoubleConfig, 1.0, 1.0);
        stringRep = Strings.toString(result);
        expected = """
            {"predicted_value":1.0,"prediction_probability":1.0,"prediction_score":1.0}""";
        assertEquals(expected, stringRep);

        ClassificationConfig boolFieldConfig = new ClassificationConfig(1, null, null, null, PredictionFieldType.BOOLEAN);
        result = new ClassificationInferenceResults(1.0, null, null, Collections.emptyList(), boolFieldConfig, 1.0, 1.0);
        stringRep = Strings.toString(result);
        expected = """
            {"predicted_value":true,"prediction_probability":1.0,"prediction_score":1.0}""";
        assertEquals(expected, stringRep);

        ClassificationConfig config = new ClassificationConfig(1);
        result = new ClassificationInferenceResults(1.0, "label1", null, Collections.emptyList(), config, 1.0, 1.0);
        stringRep = Strings.toString(result);
        expected = """
            {"predicted_value":"label1","prediction_probability":1.0,"prediction_score":1.0}""";
        assertEquals(expected, stringRep);

        ClassificationFeatureImportance fi = new ClassificationFeatureImportance("foo", Collections.emptyList());
        TopClassEntry tp = new TopClassEntry("class", 1.0, 1.0);
        result = new ClassificationInferenceResults(
            1.0,
            "label1",
            Collections.singletonList(tp),
            Collections.singletonList(fi),
            config,
            1.0,
            1.0
        );
        stringRep = Strings.toString(result);
        expected = """
            {
              "predicted_value": "label1",
              "top_classes": [ { "class_name": "class", "class_probability": 1.0, "class_score": 1.0 } ],
              "prediction_probability": 1.0,
              "prediction_score": 1.0
            }""";
        assertEquals(XContentHelper.stripWhitespace(expected), stringRep);

        config = new ClassificationConfig(0);
        result = new ClassificationInferenceResults(1.0, "label1", Collections.emptyList(), Collections.emptyList(), config, 1.0, 1.0);
        stringRep = Strings.toString(result);
        expected = """
            {"predicted_value":"label1","prediction_probability":1.0,"prediction_score":1.0}""";
        assertEquals(expected, stringRep);
    }

    @Override
    void assertFieldValues(ClassificationInferenceResults createdInstance, IngestDocument document, String resultsField) {
        String path = resultsField + "." + createdInstance.getResultsField();
        switch (createdInstance.getPredictionFieldType()) {
            case NUMBER -> assertThat(document.getFieldValue(path, Double.class), equalTo(createdInstance.predictedValue()));
            case STRING -> assertThat(document.getFieldValue(path, String.class), equalTo(createdInstance.predictedValue()));
            case BOOLEAN -> assertThat(document.getFieldValue(path, Boolean.class), equalTo(createdInstance.predictedValue()));
        }
    }
}
