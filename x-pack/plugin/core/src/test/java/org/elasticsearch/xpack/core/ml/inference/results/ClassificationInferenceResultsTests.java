/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PredictionFieldType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ClassificationInferenceResultsTests extends AbstractWireSerializingTestCase<ClassificationInferenceResults> {

    public static ClassificationInferenceResults createRandomResults() {
        Supplier<FeatureImportance> featureImportanceCtor = randomBoolean() ?
            FeatureImportanceTests::randomClassification :
            FeatureImportanceTests::randomRegression;

        return new ClassificationInferenceResults(randomDouble(),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null :
                Stream.generate(ClassificationInferenceResultsTests::createRandomClassEntry)
                    .limit(randomIntBetween(0, 10))
                    .collect(Collectors.toList()),
            randomBoolean() ? null :
                Stream.generate(featureImportanceCtor)
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.toList()),
            ClassificationConfigTests.randomClassificationConfig());
    }

    private static ClassificationInferenceResults.TopClassEntry createRandomClassEntry() {
        return new ClassificationInferenceResults.TopClassEntry(randomAlphaOfLength(10), randomDouble(), randomDouble());
    }

    public void testWriteResultsWithClassificationLabel() {
        ClassificationInferenceResults result =
            new ClassificationInferenceResults(1.0, "foo", Collections.emptyList(), ClassificationConfig.EMPTY_PARAMS);
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        result.writeResult(document, "result_field");

        assertThat(document.getFieldValue("result_field.predicted_value", String.class), equalTo("foo"));
    }

    public void testWriteResultsWithoutClassificationLabel() {
        ClassificationInferenceResults result = new ClassificationInferenceResults(1.0,
            null,
            Collections.emptyList(),
            ClassificationConfig.EMPTY_PARAMS);
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        result.writeResult(document, "result_field");

        assertThat(document.getFieldValue("result_field.predicted_value", String.class), equalTo("1.0"));
    }

    @SuppressWarnings("unchecked")
    public void testWriteResultsWithTopClasses() {
        List<ClassificationInferenceResults.TopClassEntry> entries = Arrays.asList(
            new ClassificationInferenceResults.TopClassEntry("foo", 0.7, 0.7),
            new ClassificationInferenceResults.TopClassEntry("bar", 0.2, 0.2),
            new ClassificationInferenceResults.TopClassEntry("baz", 0.1, 0.1));
        ClassificationInferenceResults result = new ClassificationInferenceResults(1.0,
            "foo",
            entries,
            new ClassificationConfig(3, "my_results", "bar", null, PredictionFieldType.STRING));
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        result.writeResult(document, "result_field");

        List<?> list = document.getFieldValue("result_field.bar", List.class);
        assertThat(list.size(), equalTo(3));

        for(int i = 0; i < 3; i++) {
            Map<String, Object> map = (Map<String, Object>)list.get(i);
            assertThat(map, equalTo(entries.get(i).asValueMap()));
        }

        assertThat(document.getFieldValue("result_field.my_results", String.class), equalTo("foo"));
    }

    public void testWriteResultsWithImportance() {
        Supplier<FeatureImportance> featureImportanceCtor = randomBoolean() ?
            FeatureImportanceTests::randomClassification :
            FeatureImportanceTests::randomRegression;

        List<FeatureImportance> importanceList = Stream.generate(featureImportanceCtor)
            .limit(5)
            .collect(Collectors.toList());
        ClassificationInferenceResults result = new ClassificationInferenceResults(0.0,
            "foo",
            Collections.emptyList(),
            importanceList,
            new ClassificationConfig(0, "predicted_value", "top_classes", 3, PredictionFieldType.STRING));
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        result.writeResult(document, "result_field");

        assertThat(document.getFieldValue("result_field.predicted_value", String.class), equalTo("foo"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> writtenImportance = (List<Map<String, Object>>)document.getFieldValue(
            "result_field.feature_importance",
            List.class);
        assertThat(writtenImportance, hasSize(3));
        importanceList.sort((l, r)-> Double.compare(Math.abs(r.getImportance()), Math.abs(l.getImportance())));
        for (int i = 0; i < 3; i++) {
            Map<String, Object> objectMap = writtenImportance.get(i);
            FeatureImportance importance = importanceList.get(i);
            assertThat(objectMap.get("feature_name"), equalTo(importance.getFeatureName()));
            assertThat(objectMap.get("importance"), equalTo(importance.getImportance()));
            if (importance.getClassImportance() != null) {
                importance.getClassImportance().forEach((k, v) -> assertThat(objectMap.get(k), equalTo(v)));
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
}
