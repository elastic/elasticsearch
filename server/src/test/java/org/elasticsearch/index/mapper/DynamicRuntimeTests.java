/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;

import java.io.IOException;

public class DynamicRuntimeTests extends MapperServiceTestCase {

    public void testDynamicLeafFields() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> b.field("dynamic", ObjectMapper.Dynamic.RUNTIME)));
        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.field("long", 123);
            b.field("double", 123.456);
            b.field("string", "text");
            b.field("boolean", true);
            b.field("date", "2020-12-15");
        }));
        assertEquals(
            "{\"_doc\":{\"dynamic\":\"runtime\","
                + "\"runtime\":{\"boolean\":{\"type\":\"boolean\"},"
                + "\"date\":{\"type\":\"date\"},"
                + "\"double\":{\"type\":\"double\"},"
                + "\"long\":{\"type\":\"long\"},"
                + "\"string\":{\"type\":\"keyword\"}}}}",
            Strings.toString(doc.dynamicMappingsUpdate())
        );
    }

    public void testWithDynamicDateFormats() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", ObjectMapper.Dynamic.RUNTIME);
            b.array("dynamic_date_formats", "dd/MM/yyyy", "dd-MM-yyyy");
        }));
        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.field("date1", "15/12/2020");
            b.field("date2", "15-12-2020");
        }));
        assertEquals(
            "{\"_doc\":{\"dynamic\":\"runtime\","
                + "\"runtime\":{\"date1\":{\"type\":\"date\",\"format\":\"dd/MM/yyyy\"},"
                + "\"date2\":{\"type\":\"date\",\"format\":\"dd-MM-yyyy\"}}}}",
            Strings.toString(doc.dynamicMappingsUpdate())
        );
    }

    public void testWithObjects() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            b.startObject("dynamic_true").field("type", "object").field("dynamic", true).endObject();
            b.startObject("dynamic_runtime").field("type", "object").field("dynamic", ObjectMapper.Dynamic.RUNTIME).endObject();
            b.endObject();
        }));
        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.startObject("anything").field("field", "text").endObject();
            b.startObject("dynamic_true").field("field1", "text").startObject("child").field("field2", "text").endObject().endObject();
            b.startObject("dynamic_runtime").field("field3", "text").startObject("child").field("field4", "text").endObject().endObject();
        }));
        assertEquals(
            "{\"_doc\":{\"dynamic\":\"false\","
                + "\"runtime\":{\"dynamic_runtime.child.field4\":{\"type\":\"keyword\"},"
                + "\"dynamic_runtime.field3\":{\"type\":\"keyword\"}},"
                + "\"properties\":{"
                + "\"dynamic_true\":{\"dynamic\":\"true\",\"properties\":{\"child\":{\"properties\":{"
                + "\"field2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}},"
                + "\"field1\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}}}}",
            Strings.toString(doc.dynamicMappingsUpdate())
        );
    }

    public void testWithDynamicTemplate() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", ObjectMapper.Dynamic.RUNTIME);
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match_mapping_type", "string");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument parsedDoc = documentMapper.parse(source(b -> {
            b.field("s", "hello");
            b.field("l", 1);
        }));
        assertEquals(
            "{\"_doc\":{\"dynamic\":\"runtime\","
                + "\"runtime\":{\"l\":{\"type\":\"long\"}},"
                + "\"properties\":{\"s\":{\"type\":\"keyword\"}}}}",
            Strings.toString(parsedDoc.dynamicMappingsUpdate())
        );
    }

    public void testDotsInFieldNames() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> b.field("dynamic", ObjectMapper.Dynamic.RUNTIME)));
        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.field("one.two.three.four", "1234");
            b.field("one.two.three", 123);
            b.array("one.two", 1.2, 1.2, 1.2);
            b.field("one", "one");
        }));
        assertEquals("{\"_doc\":{\"dynamic\":\"runtime\",\"runtime\":{" +
                "\"one\":{\"type\":\"keyword\"}," +
                "\"one.two\":{\"type\":\"double\"}," +
                "\"one.two.three\":{\"type\":\"long\"}," +
                "\"one.two.three.four\":{\"type\":\"keyword\"}}}}",
            Strings.toString(doc.dynamicMappingsUpdate())
        );
    }
}
