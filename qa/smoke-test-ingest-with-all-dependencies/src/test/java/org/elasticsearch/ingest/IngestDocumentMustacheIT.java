/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    public void testAccessMetaDataViaTemplate() {
        Map<String, Object> document = new HashMap<>();
        document.put("foo", "bar");
        IngestDocument ingestDocument = new IngestDocument("index", "type", "id", null, null, null, document);
        ingestDocument.setFieldValue(compile("field1"), ValueSource.wrap("1 {{foo}}", scriptService));
        assertThat(ingestDocument.getFieldValue("field1", String.class), equalTo("1 bar"));

        ingestDocument.setFieldValue(compile("field1"), ValueSource.wrap("2 {{_source.foo}}", scriptService));
        assertThat(ingestDocument.getFieldValue("field1", String.class), equalTo("2 bar"));
    }

    public void testAccessMapMetaDataViaTemplate() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> innerObject = new HashMap<>();
        innerObject.put("bar", "hello bar");
        innerObject.put("baz", "hello baz");
        innerObject.put("qux", Collections.singletonMap("fubar", "hello qux and fubar"));
        document.put("foo", innerObject);
        IngestDocument ingestDocument = new IngestDocument("index", "type", "id", null, null, null, document);
        ingestDocument.setFieldValue(compile("field1"),
                ValueSource.wrap("1 {{foo.bar}} {{foo.baz}} {{foo.qux.fubar}}", scriptService));
        assertThat(ingestDocument.getFieldValue("field1", String.class), equalTo("1 hello bar hello baz hello qux and fubar"));

        ingestDocument.setFieldValue(compile("field1"),
                ValueSource.wrap("2 {{_source.foo.bar}} {{_source.foo.baz}} {{_source.foo.qux.fubar}}", scriptService));
        assertThat(ingestDocument.getFieldValue("field1", String.class), equalTo("2 hello bar hello baz hello qux and fubar"));
    }

    public void testAccessListMetaDataViaTemplate() {
        Map<String, Object> document = new HashMap<>();
        document.put("list1", Arrays.asList("foo", "bar", null));
        List<Map<String, Object>> list = new ArrayList<>();
        Map<String, Object> value = new HashMap<>();
        value.put("field", "value");
        list.add(value);
        list.add(null);
        document.put("list2", list);
        IngestDocument ingestDocument = new IngestDocument("index", "type", "id", null, null, null, document);
        ingestDocument.setFieldValue(compile("field1"), ValueSource.wrap("1 {{list1.0}} {{list2.0}}", scriptService));
        assertThat(ingestDocument.getFieldValue("field1", String.class), equalTo("1 foo {field=value}"));
    }

    public void testAccessIngestMetadataViaTemplate() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> ingestMap = new HashMap<>();
        ingestMap.put("timestamp", "bogus_timestamp");
        document.put("_ingest", ingestMap);
        IngestDocument ingestDocument = new IngestDocument("index", "type", "id", null, null, null, document);
        ingestDocument.setFieldValue(compile("ingest_timestamp"),
                ValueSource.wrap("{{_ingest.timestamp}} and {{_source._ingest.timestamp}}", scriptService));
        assertThat(ingestDocument.getFieldValue("ingest_timestamp", String.class),
                equalTo(ingestDocument.getIngestMetadata().get("timestamp") + " and bogus_timestamp"));
    }
}
