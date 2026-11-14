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
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;

import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class DateIndexNameFactoryTests extends ESTestCase {

    public void testDefaults() throws Exception {
        DateIndexNameProcessor.Factory factory = new DateIndexNameProcessor.Factory(TestTemplateService.instance());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("date_rounding", "y");

        DateIndexNameProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getDateFormats().size(), equalTo(1));
        assertThat(processor.getField(), hasToString("_field"));
        assertThat(processor.getIndexNamePrefixTemplate(), hasToString(""));
        assertThat(processor.getDateRoundingTemplate(), hasToString("y"));
        assertThat(processor.getIndexNameFormatTemplate(), hasToString("yyyy-MM-dd"));
        assertThat(processor.getTimezone(), equalTo(ZoneOffset.UTC));
    }

    public void testSpecifyOptionalSettings() throws Exception {
        DateIndexNameProcessor.Factory factory = new DateIndexNameProcessor.Factory(TestTemplateService.instance());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("index_name_prefix", "_prefix");
        config.put("date_rounding", "y");
        config.put("date_formats", List.of("UNIX", "UNIX_MS"));

        DateIndexNameProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getDateFormats().size(), equalTo(2));

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("index_name_prefix", "_prefix");
        config.put("date_rounding", "y");
        config.put("index_name_format", "yyyyMMdd");

        processor = factory.create(null, null, null, config, null);
        assertThat(processor.getIndexNameFormatTemplate(), hasToString("yyyyMMdd"));

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("index_name_prefix", "_prefix");
        config.put("date_rounding", "y");
        config.put("timezone", "+02:00");

        processor = factory.create(null, null, null, config, null);
        assertThat(processor.getTimezone(), equalTo(ZoneOffset.ofHours(2)));

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("index_name_prefix", "_prefix");
        config.put("date_rounding", "y");

        processor = factory.create(null, null, null, config, null);
        assertThat(processor.getIndexNamePrefixTemplate(), hasToString("_prefix"));
    }

    public void testRequiredFields() throws Exception {
        DateIndexNameProcessor.Factory factory = new DateIndexNameProcessor.Factory(TestTemplateService.instance());
        Map<String, Object> config = new HashMap<>();
        config.put("date_rounding", "y");
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));

        config.clear();
        config.put("field", "_field");
        e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config, null));
        assertThat(e.getMessage(), equalTo("[date_rounding] required property is missing"));
    }

    /**
     * Pipelines are stored, so a malformed path in one has to be accepted when the processor is created, with the failure only coming
     * when a document is run through it. This goes through the factory, as creating a processor from a stored pipeline does.
     */
    public void testCreateWithMalformedField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "a..b");
        config.put("date_rounding", "y");
        DateIndexNameProcessor processor = new DateIndexNameProcessor.Factory(TestTemplateService.instance()).create(
            null,
            randomAlphaOfLength(10),
            null,
            config,
            null
        );
        assertThat(processor.getField(), hasToString("a..b"));

        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
    }
}
