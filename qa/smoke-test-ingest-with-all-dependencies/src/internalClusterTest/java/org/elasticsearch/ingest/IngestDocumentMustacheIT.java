/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class IngestDocumentMustacheIT extends AbstractScriptTestCase {

    public void testAccessMetadataViaTemplate() {
        Map<String, Object> document = new HashMap<>();
        document.put("foo", "bar");
        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);
        ingestDocument.setFieldValue(ingestDocument.renderTemplate(compile("field1")), ValueSource.wrap("1 {{foo}}", scriptService));
        assertThat(ingestDocument.getFieldValue("field1", String.class), equalTo("1 bar"));

        ingestDocument.setFieldValue(
            ingestDocument.renderTemplate(compile("field1")),
            ValueSource.wrap("2 {{_source.foo}}", scriptService)
        );
        assertThat(ingestDocument.getFieldValue("field1", String.class), equalTo("2 bar"));
    }

    public void testAccessMapMetadataViaTemplate() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> innerObject = new HashMap<>();
        innerObject.put("bar", "hello bar");
        innerObject.put("baz", "hello baz");
        innerObject.put("qux", Collections.singletonMap("fubar", "hello qux and fubar"));
        document.put("foo", innerObject);
        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);
        ingestDocument.setFieldValue(
            ingestDocument.renderTemplate(compile("field1")),
            ValueSource.wrap("1 {{foo.bar}} {{foo.baz}} {{foo.qux.fubar}}", scriptService)
        );
        assertThat(ingestDocument.getFieldValue("field1", String.class), equalTo("1 hello bar hello baz hello qux and fubar"));

        ingestDocument.setFieldValue(
            ingestDocument.renderTemplate(compile("field1")),
            ValueSource.wrap("2 {{_source.foo.bar}} {{_source.foo.baz}} {{_source.foo.qux.fubar}}", scriptService)
        );
        assertThat(ingestDocument.getFieldValue("field1", String.class), equalTo("2 hello bar hello baz hello qux and fubar"));
    }

    public void testAccessListMetadataViaTemplate() {
        Map<String, Object> document = new HashMap<>();
        document.put("list1", Arrays.asList("foo", "bar", null));
        List<Map<String, Object>> list = new ArrayList<>();
        Map<String, Object> value = new HashMap<>();
        value.put("field", "value");
        list.add(value);
        list.add(null);
        document.put("list2", list);
        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);
        ingestDocument.setFieldValue(
            ingestDocument.renderTemplate(compile("field1")),
            ValueSource.wrap("1 {{list1.0}} {{list2.0}}", scriptService)
        );
        assertThat(ingestDocument.getFieldValue("field1", String.class), equalTo("1 foo {field=value}"));
    }

    public void testAccessIngestMetadataViaTemplate() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> ingestMap = new HashMap<>();
        ingestMap.put("timestamp", "bogus_timestamp");
        document.put("_ingest", ingestMap);
        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);
        ingestDocument.setFieldValue(
            ingestDocument.renderTemplate(compile("ingest_timestamp")),
            ValueSource.wrap("{{_ingest.timestamp}} and {{_source._ingest.timestamp}}", scriptService)
        );
        assertThat(
            ingestDocument.getFieldValue("ingest_timestamp", String.class),
            equalTo(ingestDocument.getIngestMetadata().get("timestamp") + " and bogus_timestamp")
        );
    }
}
