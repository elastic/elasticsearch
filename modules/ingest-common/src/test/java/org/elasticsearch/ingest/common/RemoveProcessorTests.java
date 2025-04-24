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
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.common.RemoveProcessor.shouldKeep;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class RemoveProcessorTests extends ESTestCase {

    public void testRemoveFields() throws Exception {
        IngestDocument document = RandomDocumentPicks.randomIngestDocument(random());
        String field = RandomDocumentPicks.randomExistingFieldName(random(), document);
        Processor processor = new RemoveProcessor(
            randomAlphaOfLength(10),
            null,
            List.of(new TestTemplateService.MockTemplateScript.Factory(field)),
            List.of(),
            false
        );
        processor.execute(document);
        assertThat(document.hasField(field), is(false));
    }

    public void testRemoveNonExistingField() throws Exception {
        IngestDocument document = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        String tag = randomAlphaOfLength(10);
        Processor processor = new RemoveProcessor.Factory(TestTemplateService.instance()).create(null, tag, null, config, null);
        try {
            processor.execute(document);
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testIgnoreMissing() throws Exception {
        IngestDocument document = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("ignore_missing", true);
        String tag = randomAlphaOfLength(10);
        Processor processor = new RemoveProcessor.Factory(TestTemplateService.instance()).create(null, tag, null, config, null);
        processor.execute(document);
    }

    public void testIgnoreMissingAndNullInPath() throws Exception {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> some = new HashMap<>();
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> path = new HashMap<>();

        switch (randomIntBetween(0, 6)) {
            case 0 -> {
                // empty source
            }
            case 1 -> {
                source.put("some", null);
            }
            case 2 -> {
                some.put("map", null);
                source.put("some", some);
            }
            case 3 -> {
                some.put("map", map);
                source.put("some", some);
            }
            case 4 -> {
                map.put("path", null);
                some.put("map", map);
                source.put("some", some);
            }
            case 5 -> {
                map.put("path", path);
                some.put("map", map);
                source.put("some", some);
            }
            case 6 -> {
                map.put("path", "foobar");
                some.put("map", map);
                source.put("some", some);
            }
            default -> throw new AssertionError("failure, got illegal switch case");
        }
        IngestDocument document = RandomDocumentPicks.randomIngestDocument(random(), source);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "some.map.path");
        config.put("ignore_missing", true);
        Processor processor = new RemoveProcessor.Factory(TestTemplateService.instance()).create(null, null, null, config, null);
        processor.execute(document);
        assertThat(document.hasField("some.map.path"), is(false));
    }

    public void testIgnoreMissingAndNonIntegerInPath() throws Exception {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> some = new HashMap<>();
        List<Object> array = new ArrayList<>();
        Map<String, Object> path = new HashMap<>();

        switch (randomIntBetween(0, 6)) {
            case 0 -> {
                // empty source
            }
            case 1 -> {
                source.put("some", null);
            }
            case 2 -> {
                some.put("array", null);
                source.put("some", some);
            }
            case 3 -> {
                some.put("array", array);
                source.put("some", some);
            }
            case 4 -> {
                array.add(null);
                some.put("array", array);
                source.put("some", some);
            }
            case 5 -> {
                array.add(path);
                some.put("array", array);
                source.put("some", some);
            }
            case 6 -> {
                array.add("foobar");
                some.put("array", array);
                source.put("some", some);
            }
            default -> throw new AssertionError("failure, got illegal switch case");
        }
        IngestDocument document = RandomDocumentPicks.randomIngestDocument(random(), source);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "some.array.path");
        config.put("ignore_missing", true);
        Processor processor = new RemoveProcessor.Factory(TestTemplateService.instance()).create(null, null, null, config, null);
        processor.execute(document);
        assertThat(document.hasField("some.array.path"), is(false));
    }

    public void testKeepFields() throws Exception {
        Map<String, Object> address = new HashMap<>();
        address.put("street", "Ipiranga Street");
        address.put("number", 123);

        Map<String, Object> source = new HashMap<>();
        source.put("name", "eric clapton");
        source.put("age", 55);
        source.put("address", address);

        IngestDocument document = RandomDocumentPicks.randomIngestDocument(random(), source);

        Processor processor = new RemoveProcessor(null, null, List.of(), templates("name", "address.street"), false);
        processor.execute(document);

        assertTrue(document.hasField("name"));
        assertTrue(document.hasField("address"));
        assertTrue(document.hasField("address.street"));
        assertFalse(document.hasField("age"));
        assertFalse(document.hasField("address.number"));
        assertTrue(document.hasField("_index"));
        assertTrue(document.hasField("_version"));
        assertTrue(document.hasField("_id"));
        assertTrue(document.hasField("_version_type"));
    }

    public void testShouldKeep() {
        Map<String, Object> address = new HashMap<>();
        address.put("street", "Ipiranga Street");
        address.put("number", 123);

        Map<String, Object> source = new HashMap<>();
        source.put("name", "eric clapton");
        source.put("address", address);

        IngestDocument document = RandomDocumentPicks.randomIngestDocument(random(), source);

        assertTrue(shouldKeep("name", templates("name"), document));
        assertTrue(shouldKeep("age", templates("age"), document));
        assertFalse(shouldKeep("name", templates("age"), document));
        assertTrue(shouldKeep("address", templates("address.street"), document));
        assertTrue(shouldKeep("address", templates("address.number"), document));
        assertTrue(shouldKeep("address.street", templates("address"), document));
        assertTrue(shouldKeep("address.number", templates("address"), document));
        assertTrue(shouldKeep("address", templates("address"), document));
        assertFalse(shouldKeep("address.street", templates("address.number"), document));
    }

    private static List<TemplateScript.Factory> templates(String... fields) {
        return Arrays.stream(fields).map(f -> (TemplateScript.Factory) new TestTemplateService.MockTemplateScript.Factory(f)).toList();
    }
}
