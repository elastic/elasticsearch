/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigTests;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ml.inference.results.InferenceResults.writeResult;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RegressionInferenceResultsTests extends InferenceResultsTestCase<RegressionInferenceResults> {

    public static RegressionInferenceResults createRandomResults() {
        return new RegressionInferenceResults(
            randomDouble(),
            RegressionConfigTests.randomRegressionConfig(),
            randomBoolean()
                ? Collections.emptyList()
                : Stream.generate(RegressionFeatureImportanceTests::createRandomInstance)
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.toList())
        );
    }

    public void testWriteResultsWithImportance() {
        List<RegressionFeatureImportance> importanceList = Stream.generate(RegressionFeatureImportanceTests::createRandomInstance)
            .limit(5)
            .collect(Collectors.toList());
        RegressionInferenceResults result = new RegressionInferenceResults(0.3, new RegressionConfig("predicted_value", 3), importanceList);
        IngestDocument document = TestIngestDocument.emptyIngestDocument();
        writeResult(result, document, "result_field", "test");

        assertThat(document.getFieldValue("result_field.predicted_value", Double.class), equalTo(0.3));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> writtenImportance = (List<Map<String, Object>>) document.getFieldValue(
            "result_field.feature_importance",
            List.class
        );
        assertThat(writtenImportance, hasSize(3));
        importanceList.sort((l, r) -> Double.compare(Math.abs(r.getImportance()), Math.abs(l.getImportance())));
        for (int i = 0; i < 3; i++) {
            Map<String, Object> objectMap = writtenImportance.get(i);
            RegressionFeatureImportance importance = importanceList.get(i);
            assertThat(objectMap.get("feature_name"), equalTo(importance.getFeatureName()));
            assertThat(objectMap.get("importance"), equalTo(importance.getImportance()));
            assertThat(objectMap.size(), equalTo(2));
        }
    }

    @Override
    protected RegressionInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected RegressionInferenceResults mutateInstance(RegressionInferenceResults instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<RegressionInferenceResults> instanceReader() {
        return RegressionInferenceResults::new;
    }

    public void testToXContent() {
        String resultsField = "ml.results";
        RegressionInferenceResults result = new RegressionInferenceResults(1.0, resultsField);
        String stringRep = Strings.toString(result);
        String expected = "{\"" + resultsField + "\":1.0}";
        assertEquals(expected, stringRep);

        RegressionFeatureImportance fi = new RegressionFeatureImportance("foo", 1.0);
        result = new RegressionInferenceResults(1.0, resultsField, Collections.singletonList(fi));
        stringRep = Strings.toString(result);
        expected = Strings.format("""
            {"%s":1.0,"feature_importance":[{"feature_name":"foo","importance":1.0}]}\
            """, resultsField);
        assertEquals(expected, stringRep);
    }

    @Override
    void assertFieldValues(RegressionInferenceResults createdInstance, IngestDocument document, String resultsField) {
        assertThat(
            document.getFieldValue(resultsField + "." + createdInstance.getResultsField(), Double.class),
            closeTo(createdInstance.value(), 1e-10)
        );
    }
}
