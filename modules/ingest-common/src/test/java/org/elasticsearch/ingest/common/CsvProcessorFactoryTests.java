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
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CsvProcessorFactoryTests extends ESTestCase {

    public void testProcessorIsCreated() {
        CsvProcessor.Factory factory = new CsvProcessor.Factory();
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("field", "field");
        properties.put("target_fields", List.of("target"));
        properties.put("quote", "|");
        properties.put("separator", "/");
        properties.put("empty_value", "empty");
        properties.put("trim", true);
        properties.put("ignore_missing", true);
        CsvProcessor csv = factory.create(null, "csv", null, properties, null);
        assertThat(csv, notNullValue());
        assertThat(csv.field, hasToString("field"));
        assertThat(csv.headers, equalTo(new String[] { "target" }));
        assertThat(csv.quote, equalTo('|'));
        assertThat(csv.separator, equalTo('/'));
        assertThat(csv.emptyValue, equalTo("empty"));
        assertThat(csv.trim, equalTo(true));
        assertThat(csv.ignoreMissing, equalTo(true));
        assertThat(properties, is(Map.of()));
    }

    /**
     * Pipelines are stored, so a malformed path in one has to be accepted when the processor is created, with the failure only coming
     * when a document is run through it. This goes through the factory, as creating a processor from a stored pipeline does.
     */
    public void testCreateWithMalformedField() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "a..b");
        config.put("target_fields", List.of("target"));
        CsvProcessor processor = new CsvProcessor.Factory().create(null, "csv", null, config, null);
        assertThat(processor.field, hasToString("a..b"));

        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
    }

}
