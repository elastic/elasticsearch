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

package org.elasticsearch.index.mapper.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class StoredNumericValuesTests extends ESSingleNodeTestCase {
    public void testBytesAndNumericRepresentation() throws Exception {
        IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type")
                        .startObject("properties")
                            .startObject("field1").field("type", "byte").field("store", true).endObject()
                            .startObject("field2").field("type", "short").field("store", true).endObject()
                            .startObject("field3").field("type", "integer").field("store", true).endObject()
                            .startObject("field4").field("type", "float").field("store", true).endObject()
                            .startObject("field5").field("type", "long").field("store", true).endObject()
                            .startObject("field6").field("type", "double").field("store", true).endObject()
                            .startObject("field7").field("type", "ip").field("store", true).endObject()
                            .startObject("field8").field("type", "ip").field("store", true).endObject()
                            .startObject("field9").field("type", "date").field("store", true).endObject()
                            .startObject("field10").field("type", "boolean").field("store", true).endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .string();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE, false);

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
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
                .endObject()
                .bytes());

        writer.addDocument(doc.rootDoc());

        DirectoryReader reader = DirectoryReader.open(writer);
        IndexSearcher searcher = new IndexSearcher(reader);

        CustomFieldsVisitor fieldsVisitor = new CustomFieldsVisitor(
                Collections.emptySet(), Collections.singletonList("field*"), false);
        searcher.doc(0, fieldsVisitor);
        fieldsVisitor.postProcess(mapperService);
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

        reader.close();
        writer.close();
    }
}
