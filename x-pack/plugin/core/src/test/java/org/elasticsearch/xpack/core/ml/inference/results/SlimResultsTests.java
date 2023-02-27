/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SlimResultsTests extends InferenceResultsTestCase<SlimResults> {
    @Override
    protected Writeable.Reader<SlimResults> instanceReader() {
        return SlimResults::new;
    }

    @Override
    protected SlimResults createTestInstance() {
        int numTokens = randomIntBetween(0, 20);
        List<SlimResults.WeightedToken> tokenList = new ArrayList<>();
        for (int i = 0; i < numTokens; i++) {
            tokenList.add(new SlimResults.WeightedToken(i, (float) randomDoubleBetween(0.0, 5.0, false)));
        }
        return new SlimResults(randomAlphaOfLength(4), tokenList, randomBoolean());
    }

    @Override
    protected SlimResults mutateInstance(SlimResults instance) {
        return new SlimResults(instance.getResultsField() + "-FOO", instance.getWeightedTokens(), instance.isTruncated() == false);
    }

    @Override
    @SuppressWarnings("unchecked")
    void assertFieldValues(SlimResults createdInstance, IngestDocument document, String resultsField) {
        var ingestedTokens = (List<Map<String, Object>>) document.getFieldValue(
            resultsField + '.' + createdInstance.getResultsField(),
            List.class
        );
        var originalTokens = createdInstance.getWeightedTokens();
        assertEquals(originalTokens.size(), ingestedTokens.size());
        for (int i = 0; i < createdInstance.getWeightedTokens().size(); i++) {
            assertEquals(
                originalTokens.get(i).weight(),
                (float) ingestedTokens.get(i).get(Integer.toString(originalTokens.get(i).token())),
                0.0001
            );
        }
    }
}
