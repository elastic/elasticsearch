/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

public class TrimProcessorTests extends AbstractStringProcessorTestCase<String> {

    @Override
    protected AbstractStringProcessor<String> newProcessor(String field, boolean ignoreMissing, String targetField) {
        return new TrimProcessor(randomAlphaOfLength(10), null, field, ignoreMissing, targetField);
    }

    @Override
    protected String modifyInput(String input) {
        String updatedFieldValue = "";
        updatedFieldValue = addWhitespaces(updatedFieldValue);
        updatedFieldValue += input;
        updatedFieldValue = addWhitespaces(updatedFieldValue);
        return updatedFieldValue;
    }

    @Override
    protected String expectedResult(String input) {
        return input.trim();
    }

    private static String addWhitespaces(String input) {
        int prefixLength = randomIntBetween(0, 10);
        for (int i = 0; i < prefixLength; i++) {
            input += ' ';
        }
        return input;
    }
}
