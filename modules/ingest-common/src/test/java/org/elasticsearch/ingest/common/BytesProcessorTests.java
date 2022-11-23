/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.hamcrest.CoreMatchers;

import static org.hamcrest.Matchers.equalTo;

public class BytesProcessorTests extends AbstractStringProcessorTestCase<Long> {

    private String modifiedInput;

    @Override
    protected AbstractStringProcessor<Long> newProcessor(String field, boolean ignoreMissing, String targetField) {
        return new BytesProcessor(randomAlphaOfLength(10), null, field, ignoreMissing, targetField);
    }

    @Override
    protected String modifyInput(String input) {
        // largest value that allows all results < Long.MAX_VALUE bytes
        long randomNumber = randomLongBetween(1, Long.MAX_VALUE / ByteSizeUnit.PB.toBytes(1));
        ByteSizeUnit randomUnit = randomFrom(ByteSizeUnit.values());
        modifiedInput = randomNumber + randomUnit.getSuffix();
        return modifiedInput;
    }

    @Override
    protected Long expectedResult(String input) {
        return ByteSizeValue.parseBytesSizeValue(modifiedInput, null, "").getBytes();
    }

    @Override
    protected Class<Long> expectedResultType() {
        return Long.class;
    }

    public void testTooLarge() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "8912pb");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> processor.execute(ingestDocument));
        assertThat(
            exception.getMessage(),
            CoreMatchers.equalTo("failed to parse setting [Ingest Field] with value [8912pb] as a size in bytes")
        );
        assertThat(
            exception.getCause().getMessage(),
            CoreMatchers.containsString("Values greater than 9223372036854775807 bytes are not supported")
        );
    }

    public void testNotBytes() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "junk");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), CoreMatchers.equalTo("failed to parse setting [Ingest Field] with value [junk]"));
    }

    public void testMissingUnits() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "1");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), CoreMatchers.containsString("unit is missing or unrecognized"));
    }

    public void testFractional() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "1.1kb");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, expectedResultType()), equalTo(1126L));
        assertWarnings(
            "Fractional bytes values are deprecated. Use non-fractional bytes values instead: [1.1kb] found for setting " + "[Ingest Field]"
        );
    }
}
