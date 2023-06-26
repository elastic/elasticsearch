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
import java.util.stream.Collectors;

public class TextExpansionResultsTests extends InferenceResultsTestCase<TextExpansionResults> {
    public static TextExpansionResults createRandomResults() {
        int numTokens = randomIntBetween(0, 20);
        List<TextExpansionResults.WeightedToken> tokenList = new ArrayList<>();
        for (int i = 0; i < numTokens; i++) {
            tokenList.add(new TextExpansionResults.WeightedToken(Integer.toString(i), (float) randomDoubleBetween(0.0, 5.0, false)));
        }
        return new TextExpansionResults(randomAlphaOfLength(4), tokenList, randomBoolean());
    }

    @Override
    protected Writeable.Reader<TextExpansionResults> instanceReader() {
        return TextExpansionResults::new;
    }

    @Override
    protected TextExpansionResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected TextExpansionResults mutateInstance(TextExpansionResults instance) {
        return new TextExpansionResults(instance.getResultsField() + "-FOO", instance.getWeightedTokens(), instance.isTruncated() == false);
    }

    @Override
    @SuppressWarnings("unchecked")
    void assertFieldValues(TextExpansionResults createdInstance, IngestDocument document, String resultsField) {
        var ingestedTokens = (Map<String, Object>) document.getFieldValue(
            resultsField + '.' + createdInstance.getResultsField(),
            Map.class
        );
        var tokenMap = createdInstance.getWeightedTokens()
            .stream()
            .collect(Collectors.toMap(TextExpansionResults.WeightedToken::token, TextExpansionResults.WeightedToken::weight));
        assertEquals(tokenMap.size(), ingestedTokens.size());

        assertEquals(tokenMap, ingestedTokens);
    }
}
