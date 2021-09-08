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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class FillMaskResultsTests extends AbstractWireSerializingTestCase<FillMaskResults> {
    @Override
    protected Writeable.Reader<FillMaskResults> instanceReader() {
        return FillMaskResults::new;
    }

    @Override
    protected FillMaskResults createTestInstance() {
        int numResults = randomIntBetween(0, 3);
        List<FillMaskResults.Prediction> resultList = new ArrayList<>();
        for (int i=0; i<numResults; i++) {
            resultList.add(new FillMaskResults.Prediction(randomAlphaOfLength(4), randomDouble(), randomAlphaOfLength(4)));
        }
        return new FillMaskResults(resultList);
    }

    @SuppressWarnings("unchecked")
    public void testAsMap() {
        FillMaskResults testInstance = createTestInstance();
        Map<String, Object> asMap = testInstance.asMap();
        List<Map<String, Object>> resultList = (List<Map<String, Object>>)asMap.get("results");
        assertThat(resultList, hasSize(testInstance.getPredictions().size()));
        for (int i = 0; i<testInstance.getPredictions().size(); i++) {
            FillMaskResults.Prediction result = testInstance.getPredictions().get(i);
            Map<String, Object> map = resultList.get(i);
            assertThat(map.get("score"), equalTo(result.getScore()));
            assertThat(map.get("token"), equalTo(result.getToken()));
            assertThat(map.get("sequence"), equalTo(result.getSequence()));
        }
    }
}
