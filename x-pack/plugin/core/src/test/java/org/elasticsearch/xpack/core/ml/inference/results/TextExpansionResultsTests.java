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

public class TextExpansionResultsTests extends InferenceResultsTestCase<TextExpansionResults> {
    @Override
    protected Writeable.Reader<TextExpansionResults> instanceReader() {
        return TextExpansionResults::new;
    }

    @Override
    protected TextExpansionResults createTestInstance() {
        int numTokens = randomIntBetween(0, 20);
        List<TextExpansionResults.WeightedToken> tokenList = new ArrayList<>();
        for (int i = 0; i < numTokens; i++) {
            tokenList.add(new TextExpansionResults.WeightedToken(i, (float) randomDoubleBetween(0.0, 5.0, false)));
        }
        return new TextExpansionResults(randomAlphaOfLength(4), tokenList, randomBoolean());
    }

    @Override
    protected TextExpansionResults mutateInstance(TextExpansionResults instance) {
        return new TextExpansionResults(instance.getResultsField() + "-FOO", instance.getWeightedTokens(), instance.isTruncated() == false);
    }

    @Override
    @SuppressWarnings("unchecked")
    void assertFieldValues(TextExpansionResults createdInstance, IngestDocument document, String resultsField) {
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
