/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasSize;

public class SentimentAnalysisResultsTests extends AbstractWireSerializingTestCase<SentimentAnalysisResults> {
    @Override
    protected Writeable.Reader<SentimentAnalysisResults> instanceReader() {
        return SentimentAnalysisResults::new;
    }

    @Override
    protected SentimentAnalysisResults createTestInstance() {
        return new SentimentAnalysisResults(randomDouble(), randomDouble());
    }

    public void testAsMap() {
        SentimentAnalysisResults testInstance = createTestInstance();
        Map<String, Object> asMap = testInstance.asMap();
        assertThat(asMap.keySet(), hasSize(2));
        assertThat(testInstance.getPositiveScore(),
            closeTo((Double)asMap.get(SentimentAnalysisResults.POSITIVE_SCORE), 0.0001));
        assertThat(testInstance.getNegativeScore(),
            closeTo((Double)asMap.get(SentimentAnalysisResults.NEGATIVE_SCORE), 0.0001));
    }
}
