/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoveProcessorTests extends ESTestCase {

    public void testRemoveFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String field = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        Processor processor = new RemoveProcessor(
            randomAlphaOfLength(10),
            null,
            List.of(new TestTemplateService.MockTemplateScript.Factory(field)),
            List.of(),
            false
        );
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(field), equalTo(false));
    }

    public void testRemoveNonExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        String processorTag = randomAlphaOfLength(10);
        Processor processor = new RemoveProcessor.Factory(TestTemplateService.instance()).create(null, processorTag, null, config);
        try {
            processor.execute(ingestDocument);
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testIgnoreMissing() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        Processor processor = new RemoveProcessor.Factory(TestTemplateService.instance()).create(null, processorTag, null, config);
        processor.execute(ingestDocument);
    }

    public void testKeepFields() throws Exception {
        Map<String, Object> address = new HashMap<>();
        address.put("street", "Ipiranga Street");
        address.put("number", 123);

        Map<String, Object> source = new HashMap<>();
        source.put("name", "eric clapton");
        source.put("age", 55);
        source.put("address", address);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), source);

        List<TemplateScript.Factory> fieldsToKeep = List.of(
            new TestTemplateService.MockTemplateScript.Factory("name"),
            new TestTemplateService.MockTemplateScript.Factory("address.street")
        );

        Processor processor = new RemoveProcessor(randomAlphaOfLength(10), null, new ArrayList<>(), fieldsToKeep, false);
        processor.execute(ingestDocument);
        assertTrue(ingestDocument.hasField("name"));
        assertTrue(ingestDocument.hasField("address"));
        assertTrue(ingestDocument.hasField("address.street"));
        assertFalse(ingestDocument.hasField("age"));
        assertFalse(ingestDocument.hasField("address.number"));
        assertTrue(ingestDocument.hasField("_index"));
        assertTrue(ingestDocument.hasField("_version"));
        assertTrue(ingestDocument.hasField("_id"));
        assertTrue(ingestDocument.hasField("_version_type"));
    }

    public void testShouldKeep(String a, String b) {
        Map<String, Object> address = new HashMap<>();
        address.put("street", "Ipiranga Street");
        address.put("number", 123);

        Map<String, Object> source = new HashMap<>();
        source.put("name", "eric clapton");
        source.put("address", address);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), source);

        assertTrue(RemoveProcessor.shouldKeep("name", List.of(new TestTemplateService.MockTemplateScript.Factory("name")), ingestDocument));

        assertTrue(RemoveProcessor.shouldKeep("age", List.of(new TestTemplateService.MockTemplateScript.Factory("age")), ingestDocument));

        assertFalse(RemoveProcessor.shouldKeep("name", List.of(new TestTemplateService.MockTemplateScript.Factory("age")), ingestDocument));

        assertTrue(
            RemoveProcessor.shouldKeep(
                "address",
                List.of(new TestTemplateService.MockTemplateScript.Factory("address.street")),
                ingestDocument
            )
        );

        assertTrue(
            RemoveProcessor.shouldKeep(
                "address",
                List.of(new TestTemplateService.MockTemplateScript.Factory("address.number")),
                ingestDocument
            )
        );

        assertTrue(
            RemoveProcessor.shouldKeep(
                "address.street",
                List.of(new TestTemplateService.MockTemplateScript.Factory("address")),
                ingestDocument
            )
        );

        assertTrue(
            RemoveProcessor.shouldKeep(
                "address.number",
                List.of(new TestTemplateService.MockTemplateScript.Factory("address")),
                ingestDocument
            )
        );

        assertTrue(
            RemoveProcessor.shouldKeep("address", List.of(new TestTemplateService.MockTemplateScript.Factory("address")), ingestDocument)
        );

        assertFalse(
            RemoveProcessor.shouldKeep(
                "address.street",
                List.of(new TestTemplateService.MockTemplateScript.Factory("address.number")),
                ingestDocument
            )
        );
    }

}
