/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

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

    public void testTrimLeading() throws Exception {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        ingestDocument.setFieldValue("field", "   hello world   ");
        TrimProcessor processor = new TrimProcessor(randomAlphaOfLength(10), null, "field", false, "field", TrimProcessor.TrimMode.LEADING);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("field", String.class), equalTo("hello world   "));
    }

    public void testTrimTrailing() throws Exception {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        ingestDocument.setFieldValue("field", "   hello world   ");
        TrimProcessor processor = new TrimProcessor(
            randomAlphaOfLength(10),
            null,
            "field",
            false,
            "field",
            TrimProcessor.TrimMode.TRAILING
        );
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("field", String.class), equalTo("   hello world"));
    }

    public void testTrimBoth() throws Exception {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        ingestDocument.setFieldValue("field", "   hello world   ");
        TrimProcessor processor = new TrimProcessor(randomAlphaOfLength(10), null, "field", false, "field", TrimProcessor.TrimMode.BOTH);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("field", String.class), equalTo("hello world"));
    }

    public void testTrimFactoryWithTrimMode() throws Exception {
        TrimProcessor.Factory factory = new TrimProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "foo");
        config.put("trim_mode", "leading");
        TrimProcessor processor = (TrimProcessor) factory.create(null, null, null, config, null);
        assertThat(processor.getTrimMode(), equalTo(TrimProcessor.TrimMode.LEADING));

        config.put("field", "foo");
        config.put("trim_mode", "trailing");
        processor = (TrimProcessor) factory.create(null, null, null, config, null);
        assertThat(processor.getTrimMode(), equalTo(TrimProcessor.TrimMode.TRAILING));

        config.put("field", "foo");
        config.put("trim_mode", "both");
        processor = (TrimProcessor) factory.create(null, null, null, config, null);
        assertThat(processor.getTrimMode(), equalTo(TrimProcessor.TrimMode.BOTH));
    }

    public void testTrimFactoryWithInvalidTrimModeThrows() {
        TrimProcessor.Factory factory = new TrimProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "foo");
        config.put("trim_mode", "invalid_mode");
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(
            e.getMessage(),
            equalTo("[trim_mode] trim mode [invalid_mode] not supported. Valid modes are: [both, leading, trailing].")
        );
    }

    private static String addWhitespaces(String input) {
        int prefixLength = randomIntBetween(0, 10);
        for (int i = 0; i < prefixLength; i++) {
            input += ' ';
        }
        return input;
    }
}
