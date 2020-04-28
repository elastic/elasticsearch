/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigTests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;


public class RegressionInferenceResultsTests extends AbstractWireSerializingTestCase<RegressionInferenceResults> {

    public static RegressionInferenceResults createRandomResults() {
        return new RegressionInferenceResults(randomDouble(),
            RegressionConfigTests.randomRegressionConfig(),
            randomBoolean() ? null :
                Stream.generate(FeatureImportanceTests::randomRegression)
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.toList()));
    }

    public void testWriteResults() {
        RegressionInferenceResults result = new RegressionInferenceResults(0.3, RegressionConfig.EMPTY_PARAMS);
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        result.writeResult(document, "result_field");

        assertThat(document.getFieldValue("result_field.predicted_value", Double.class), equalTo(0.3));
    }

    public void testWriteResultsWithImportance() {
        List<FeatureImportance> importanceList = Stream.generate(FeatureImportanceTests::randomRegression)
            .limit(5)
            .collect(Collectors.toList());
        RegressionInferenceResults result = new RegressionInferenceResults(0.3,
            new RegressionConfig("predicted_value", 3),
            importanceList);
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        result.writeResult(document, "result_field");

        assertThat(document.getFieldValue("result_field.predicted_value", Double.class), equalTo(0.3));
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
            assertThat(objectMap.size(), equalTo(2));
        }
    }

    @Override
    protected RegressionInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected Writeable.Reader<RegressionInferenceResults> instanceReader() {
        return RegressionInferenceResults::new;
    }
}
