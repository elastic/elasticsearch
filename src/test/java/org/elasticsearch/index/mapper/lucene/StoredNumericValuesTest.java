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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class StoredNumericValuesTest extends ElasticsearchSingleNodeTest {

    @Test
    public void testBytesAndNumericRepresentation() throws Exception {
        IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type")
                        .startObject("properties")
                            .startObject("field1").field("type", "integer").field("store", "yes").endObject()
                            .startObject("field2").field("type", "float").field("store", "yes").endObject()
                            .startObject("field3").field("type", "long").field("store", "yes").endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .string();
        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = mapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .field("field1", 1)
                    .field("field2", 1.1)
                    .startArray("field3").value(1).value(2).value(3).endArray()
                .endObject()
                .bytes());

        writer.addDocument(doc.rootDoc());

        // Indexing a doc in the old way
        FieldType fieldType = new FieldType();
        fieldType.setStored(true);
        fieldType.setNumericType(FieldType.NumericType.INT);
        Document doc2 = new Document();
        doc2.add(new StoredField("field1", new BytesRef(Numbers.intToBytes(1))));
        doc2.add(new StoredField("field2", new BytesRef(Numbers.floatToBytes(1.1f))));
        doc2.add(new StoredField("field3", new BytesRef(Numbers.longToBytes(1l))));
        doc2.add(new StoredField("field3", new BytesRef(Numbers.longToBytes(2l))));
        doc2.add(new StoredField("field3", new BytesRef(Numbers.longToBytes(3l))));
        writer.addDocument(doc2);

        DirectoryReader reader = DirectoryReader.open(writer, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        Set<String> fields = new HashSet<>(Arrays.asList("field1", "field2", "field3"));
        CustomFieldsVisitor fieldsVisitor = new CustomFieldsVisitor(fields, false);
        searcher.doc(0, fieldsVisitor);
        fieldsVisitor.postProcess(mapper);
        assertThat(fieldsVisitor.fields().size(), equalTo(3));
        assertThat(fieldsVisitor.fields().get("field1").size(), equalTo(1));
        assertThat((Integer) fieldsVisitor.fields().get("field1").get(0), equalTo(1));
        assertThat(fieldsVisitor.fields().get("field2").size(), equalTo(1));
        assertThat((Float) fieldsVisitor.fields().get("field2").get(0), equalTo(1.1f));
        assertThat(fieldsVisitor.fields().get("field3").size(), equalTo(3));
        assertThat((Long) fieldsVisitor.fields().get("field3").get(0), equalTo(1l));
        assertThat((Long) fieldsVisitor.fields().get("field3").get(1), equalTo(2l));
        assertThat((Long) fieldsVisitor.fields().get("field3").get(2), equalTo(3l));

        // Make sure the doc gets loaded as if it was stored in the new way
        fieldsVisitor.reset();
        searcher.doc(1, fieldsVisitor);
        fieldsVisitor.postProcess(mapper);
        assertThat(fieldsVisitor.fields().size(), equalTo(3));
        assertThat(fieldsVisitor.fields().get("field1").size(), equalTo(1));
        assertThat((Integer) fieldsVisitor.fields().get("field1").get(0), equalTo(1));
        assertThat(fieldsVisitor.fields().get("field2").size(), equalTo(1));
        assertThat((Float) fieldsVisitor.fields().get("field2").get(0), equalTo(1.1f));
        assertThat(fieldsVisitor.fields().get("field3").size(), equalTo(3));
        assertThat((Long) fieldsVisitor.fields().get("field3").get(0), equalTo(1l));
        assertThat((Long) fieldsVisitor.fields().get("field3").get(1), equalTo(2l));
        assertThat((Long) fieldsVisitor.fields().get("field3").get(2), equalTo(3l));

        reader.close();
        writer.close();
    }
}
