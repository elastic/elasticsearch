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
        return new SentimentAnalysisResults(randomAlphaOfLength(6), randomDouble(),
            randomAlphaOfLength(6), randomDouble());
    }

    public void testAsMap() {
        SentimentAnalysisResults testInstance = new SentimentAnalysisResults("foo", 1.0, "bar", 0.0);
        Map<String, Object> asMap = testInstance.asMap();
        assertThat(asMap.keySet(), hasSize(2));
        assertThat(1.0, closeTo((Double)asMap.get("foo"), 0.0001));
        assertThat(0.0, closeTo((Double)asMap.get("bar"), 0.0001));
    }
}
