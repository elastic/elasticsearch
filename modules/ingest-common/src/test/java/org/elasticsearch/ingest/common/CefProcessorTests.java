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
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CefProcessorTests extends ESTestCase {

    private IngestDocument ingestDocument;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Map<String, Object> source = new HashMap<>();
        source.put("message", "CEF:0|Elasticsearch|Test|1.0|100|Test Event|5|src=10.0.0.1 dst=10.0.0.2 spt=1232 dpt=80");
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExecute() {
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("deviceVendor"), equalTo("Elasticsearch"));
        assertThat(cef.get("deviceProduct"), equalTo("Test"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("Test Event"));
        assertThat(cef.get("severity"), equalTo("5"));

        Map<String, String> extensions = (Map<String, String>) cef.get("extensions");
        assertThat(extensions.get("source.ip"), equalTo("10.0.0.1"));
        assertThat(extensions.get("destination.ip"), equalTo("10.0.0.2"));
        assertThat(extensions.get("source.port"), equalTo("1232"));
        assertThat(extensions.get("destination.port"), equalTo("80"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCefFormat() {
        Map<String, Object> invalidSource = new HashMap<>();
        invalidSource.put("message", "Invalid CEF message");
        IngestDocument invalidIngestDocument = new IngestDocument("index", "id", 1L, null, null, invalidSource);

        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(invalidIngestDocument);
    }
}
