/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class AppendProcessorFactoryTests extends ESTestCase {

    private AppendProcessor.Factory factory;

    @Before
    public void init() {
        factory = new AppendProcessor.Factory(TestTemplateService.instance());
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        Object value;
        if (randomBoolean()) {
            value = "value1";
        } else {
            value = List.of("value1", "value2", "value3");
        }
        config.put("value", value);
        String processorTag = randomAlphaOfLength(10);
        AppendProcessor appendProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(appendProcessor.getTag(), equalTo(processorTag));
        assertThat(appendProcessor.getField(), hasToString("field1"));
        assertThat(appendProcessor.getValue().copyAndResolve(Map.of()), equalTo(value));
    }

    public void testCreateNoFieldPresent() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("value", "value1");
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoValuePresent() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[value] required property is missing"));
        }
    }

    public void testCreateNullValue() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("value", null);
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[value] required property is missing"));
        }
    }

    public void testInvalidMustacheTemplate() throws Exception {
        factory = new AppendProcessor.Factory(TestTemplateService.instance(true));
        Map<String, Object> config = new HashMap<>();
        config.put("field", "{{field1}}");
        config.put("value", "value1");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> factory.create(null, processorTag, null, config, null)
        );
        assertThat(exception.getMessage(), equalTo("java.lang.RuntimeException: could not compile script"));
        assertThat(exception.getMetadata("es.processor_tag").get(0), equalTo(processorTag));
    }

    public void testMediaType() throws Exception {
        // valid media type
        String expectedMediaType = randomFrom(ConfigurationUtils.VALID_MEDIA_TYPES);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("value", "value1");
        config.put("media_type", expectedMediaType);
        String processorTag = randomAlphaOfLength(10);
        AppendProcessor appendProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(appendProcessor.getTag(), equalTo(processorTag));

        // invalid media type
        expectedMediaType = randomValueOtherThanMany(
            m -> List.of(ConfigurationUtils.VALID_MEDIA_TYPES).contains(m),
            () -> randomAlphaOfLengthBetween(5, 9)
        );
        final Map<String, Object> config2 = new HashMap<>();
        config2.put("field", "field1");
        config2.put("value", "value1");
        config2.put("media_type", expectedMediaType);
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> factory.create(null, processorTag, null, config2, null)
        );
        assertThat(e.getMessage(), containsString("property does not contain a supported media type [" + expectedMediaType + "]"));
    }

    public void testCreateWithCopyFrom() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("copy_from", "field2");
        String processorTag = randomAlphaOfLength(10);
        AppendProcessor appendProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(appendProcessor.getTag(), equalTo(processorTag));
        assertThat(appendProcessor.getField(), hasToString("field1"));
        assertThat(appendProcessor.getCopyFrom(), hasToString("field2"));
    }

    public void testCreateWithCopyFromAndValue() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("copy_from", "field2");
        config.put("value", "value1");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> factory.create(null, processorTag, null, config, null)
        );
        assertThat(exception.getMessage(), equalTo("[copy_from] cannot set both `copy_from` and `value` in the same processor"));
    }

    /**
     * Pipelines are stored, so a malformed path in one has to be accepted when the processor is created, with the failure only coming
     * when a document is run through it. This goes through the factory, as creating a processor from a stored pipeline does.
     */
    public void testCreateWithMalformedField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "a..b");
        config.put("value", "value1");
        AppendProcessor processor = factory.create(null, randomAlphaOfLength(10), null, config, null);
        assertThat(processor.getField(), hasToString("a..b"));

        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
    }

    public void testCreateWithMalformedCopyFrom() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "target");
        config.put("copy_from", "a..b");
        AppendProcessor processor = factory.create(null, randomAlphaOfLength(10), null, config, null);
        assertThat(processor.getCopyFrom(), hasToString("a..b"));

        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
    }
}
