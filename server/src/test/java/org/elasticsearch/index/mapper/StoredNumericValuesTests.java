/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class StoredNumericValuesTests extends MapperServiceTestCase {
    public void testBytesAndNumericRepresentation() throws Exception {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field1").field("type", "byte").field("store", true).endObject();
            b.startObject("field2").field("type", "short").field("store", true).endObject();
            b.startObject("field3").field("type", "integer").field("store", true).endObject();
            b.startObject("field4").field("type", "float").field("store", true).endObject();
            b.startObject("field5").field("type", "long").field("store", true).endObject();
            b.startObject("field6").field("type", "double").field("store", true).endObject();
            b.startObject("field7").field("type", "ip").field("store", true).endObject();
            b.startObject("field8").field("type", "ip").field("store", true).endObject();
            b.startObject("field9").field("type", "date").field("store", true).endObject();
            b.startObject("field10").field("type", "boolean").field("store", true).endObject();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("field1", 1)
                            .field("field2", 1)
                            .field("field3", 1)
                            .field("field4", 1.1)
                            .startArray("field5").value(1).value(2).value(3).endArray()
                            .field("field6", 1.1)
                            .field("field7", "192.168.1.1")
                            .field("field8", "2001:db8::2:1")
                            .field("field9", "2016-04-05")
                            .field("field10", true)
                        .endObject()),
                XContentType.JSON));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {

            IndexSearcher searcher = new IndexSearcher(ir);

            Set<String> fieldNames = Sets.newHashSet("field1", "field2", "field3", "field4", "field5",
                "field6", "field7", "field8", "field9", "field10");
            CustomFieldsVisitor fieldsVisitor = new CustomFieldsVisitor(fieldNames, false);
            searcher.doc(0, fieldsVisitor);

            fieldsVisitor.postProcess(mapperService::fieldType);
            assertThat(fieldsVisitor.fields().size(), equalTo(10));
            assertThat(fieldsVisitor.fields().get("field1").size(), equalTo(1));
            assertThat(fieldsVisitor.fields().get("field1").get(0), equalTo((byte) 1));

            assertThat(fieldsVisitor.fields().get("field2").size(), equalTo(1));
            assertThat(fieldsVisitor.fields().get("field2").get(0), equalTo((short) 1));

            assertThat(fieldsVisitor.fields().get("field3").size(), equalTo(1));
            assertThat(fieldsVisitor.fields().get("field3").get(0), equalTo(1));

            assertThat(fieldsVisitor.fields().get("field4").size(), equalTo(1));
            assertThat(fieldsVisitor.fields().get("field4").get(0), equalTo(1.1f));

            assertThat(fieldsVisitor.fields().get("field5").size(), equalTo(3));
            assertThat(fieldsVisitor.fields().get("field5").get(0), equalTo(1L));
            assertThat(fieldsVisitor.fields().get("field5").get(1), equalTo(2L));
            assertThat(fieldsVisitor.fields().get("field5").get(2), equalTo(3L));

            assertThat(fieldsVisitor.fields().get("field6").size(), equalTo(1));
            assertThat(fieldsVisitor.fields().get("field6").get(0), equalTo(1.1));

            assertThat(fieldsVisitor.fields().get("field7").size(), equalTo(1));
            assertThat(fieldsVisitor.fields().get("field7").get(0), equalTo("192.168.1.1"));

            assertThat(fieldsVisitor.fields().get("field8").size(), equalTo(1));
            assertThat(fieldsVisitor.fields().get("field8").get(0), equalTo("2001:db8::2:1"));

            assertThat(fieldsVisitor.fields().get("field9").size(), equalTo(1));
            assertThat(fieldsVisitor.fields().get("field9").get(0), equalTo("2016-04-05T00:00:00.000Z"));

            assertThat(fieldsVisitor.fields().get("field10").size(), equalTo(1));
            assertThat(fieldsVisitor.fields().get("field10").get(0), equalTo(true));

        });
    }
}
