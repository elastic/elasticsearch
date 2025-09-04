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
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RecoverFailureDocumentProcessorTests extends ESTestCase {

    private static Processor createRecoverFailureDocumentProcessor() {
        return new RecoverFailureDocumentProcessor(randomAlphaOfLength(8), null);
    }

    private static IngestDocument createFailureDoc(
        String currentIndex,
        String originalIndex,
        String routing,
        Map<String, Object> originalSource
    ) {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        // current metadata
        sourceAndMetadata.put("_index", currentIndex);
        sourceAndMetadata.put("_id", "test-id");
        sourceAndMetadata.put("_version", 1L);

        // failure wrapper
        Map<String, Object> documentWrapper = new HashMap<>();
        documentWrapper.put("index", originalIndex);
        if (routing != null) {
            documentWrapper.put("routing", routing);
        }
        documentWrapper.put("source", originalSource);

        sourceAndMetadata.put("error", "simulated failure");
        sourceAndMetadata.put("document", documentWrapper);

        // no special ingest-metadata
        Map<String, Object> ingestMetadata = new HashMap<>();

        return new IngestDocument(sourceAndMetadata, ingestMetadata);
    }

    public void testRecoverFailureDocument_basic() throws Exception {
        Processor processor = createRecoverFailureDocumentProcessor();

        Map<String, Object> originalSource = new HashMap<>();
        originalSource.put("a", 1);
        originalSource.put("b", "x");

        IngestDocument doc = createFailureDoc("failure-index", "orig-index", null, originalSource);

        processor.execute(doc);

        // metadata index restored
        assertThat(doc.getFieldValue("_index", String.class), equalTo("orig-index"));
        // routing is not set when not provided
        assertThat(doc.hasField("_routing", true), is(false));

        // source restored at root
        assertThat(doc.getFieldValue("a", Integer.class), equalTo(1));
        assertThat(doc.getFieldValue("b", String.class), equalTo("x"));

        // failure scaffolding removed
        assertThat(doc.hasField("error", true), is(false));
        assertThat(doc.hasField("document", true), is(false));
    }

    public void testRecoverFailureDocument_withRouting() throws Exception {
        Processor processor = createRecoverFailureDocumentProcessor();

        Map<String, Object> originalSource = new HashMap<>();
        originalSource.put("nested", Map.of("k", "v"));

        IngestDocument doc = createFailureDoc("dlq-index", "original-idx", "orig-route", originalSource);

        processor.execute(doc);

        assertThat(doc.getFieldValue("_index", String.class), equalTo("original-idx"));
        assertThat(doc.getFieldValue("_routing", String.class), equalTo("orig-route"));
        assertThat(doc.getFieldValue("nested.k", String.class), equalTo("v"));
        assertThat(doc.hasField("error", true), is(false));
        assertThat(doc.hasField("document", true), is(false));
    }

    public void testRecoverFailureDocument_missingDocument_throws() {
        Processor processor = createRecoverFailureDocumentProcessor();

        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("_index", "failure-index");
        sourceAndMetadata.put("error", "simulated failure");
        sourceAndMetadata.put("_version", 1L);
        // no "document" field

        IngestDocument doc = new IngestDocument(sourceAndMetadata, new HashMap<>());

        Exception e = expectThrows(IllegalArgumentException.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), containsString("document"));
    }

    public void testRecoverFailureDocument_missingSourceInsideDocument_throws() {
        Processor processor = createRecoverFailureDocumentProcessor();

        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("_index", "failure-index");
        sourceAndMetadata.put("error", "simulated failure");
        sourceAndMetadata.put("_version", 1L);
        sourceAndMetadata.put("document", Map.of("index", "orig-index")); // missing "source"

        IngestDocument doc = new IngestDocument(sourceAndMetadata, new HashMap<>());

        Exception e = expectThrows(IllegalArgumentException.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), containsString("source"));
    }
}
