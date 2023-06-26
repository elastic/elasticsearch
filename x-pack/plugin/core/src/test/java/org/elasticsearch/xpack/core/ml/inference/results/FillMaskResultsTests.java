/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.results.InferenceResults.PREDICTION_PROBABILITY;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FillMaskResultsTests extends AbstractWireSerializingTestCase<FillMaskResults> {

    public static FillMaskResults createRandomResults() {
        int numResults = randomIntBetween(0, 3);
        List<TopClassEntry> resultList = new ArrayList<>();
        for (int i = 0; i < numResults; i++) {
            resultList.add(TopClassEntryTests.createRandomTopClassEntry());
        }
        return new FillMaskResults(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            resultList,
            DEFAULT_RESULTS_FIELD,
            randomDouble(),
            randomBoolean()
        );
    }

    @Override
    protected Writeable.Reader<FillMaskResults> instanceReader() {
        return FillMaskResults::new;
    }

    @Override
    protected FillMaskResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected FillMaskResults mutateInstance(FillMaskResults instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @SuppressWarnings("unchecked")
    public void testAsMap() {
        FillMaskResults testInstance = createTestInstance();
        Map<String, Object> asMap = testInstance.asMap();
        assertThat(asMap.get(DEFAULT_RESULTS_FIELD), equalTo(testInstance.predictedValue()));
        assertThat(asMap.get(PREDICTION_PROBABILITY), equalTo(testInstance.getPredictionProbability()));
        assertThat(asMap.get(DEFAULT_RESULTS_FIELD + "_sequence"), equalTo(testInstance.getPredictedSequence()));
        List<Map<String, Object>> resultList = (List<Map<String, Object>>) asMap.get(DEFAULT_TOP_CLASSES_RESULTS_FIELD);
        if (testInstance.isTruncated) {
            assertThat(asMap.get("is_truncated"), is(true));
        } else {
            assertThat(asMap, not(hasKey("is_truncated")));
        }
        if (testInstance.getTopClasses().size() == 0) {
            assertThat(resultList, is(nullValue()));
        } else {
            assertThat(resultList, hasSize(testInstance.getTopClasses().size()));
            for (int i = 0; i < testInstance.getTopClasses().size(); i++) {
                TopClassEntry result = testInstance.getTopClasses().get(i);
                Map<String, Object> map = resultList.get(i);
                assertThat(map.get("class_score"), equalTo(result.getScore()));
                assertThat(map.get("class_name"), equalTo(result.getClassification()));
            }
        }
    }
}
