/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;

import java.util.Map;
import java.util.regex.Pattern;

public class GsubProcessorTests extends AbstractStringProcessorTestCase<String> {

    @Override
    protected AbstractStringProcessor<String> newProcessor(String field, boolean ignoreMissing, String targetField) {
        return new GsubProcessor(randomAlphaOfLength(10), null, field, Pattern.compile("\\."), "-", ignoreMissing, targetField);
    }

    @Override
    protected String modifyInput(String input) {
        return "127.0.0.1";
    }

    @Override
    protected String expectedResult(String input) {
        return "127-0-0-1";
    }

    public void testStackOverflow() {
        // This tests that we rethrow StackOverflowErrors as ElasticsearchExceptions so that we don't take down the node
        String badRegex = "( (?=(?:[^'\"]|'[^']*'|\"[^\"]*\")*$))";
        GsubProcessor processor = new GsubProcessor(
            randomAlphaOfLength(10),
            null,
            "field",
            Pattern.compile(badRegex),
            "-",
            false,
            "targetField"
        );
        StringBuilder badSourceBuilder = new StringBuilder("key1=x key2=");
        badSourceBuilder.append("x".repeat(10000));
        Map<String, Object> source = Map.of("field", badSourceBuilder.toString());
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), source);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
    }
}
