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

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import static org.hamcrest.Matchers.containsString;

public class NumberFieldMapperTests extends AbstractNumericFieldMapperTestCase {

    @Override
    protected void setTypeList() {
        TYPES = new HashSet<>(Arrays.asList("byte", "short", "integer", "long", "float", "double"));
    }

    @Override
    public void doTestDefaults(String type) throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", 123)
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    @Override
    public void doTestNotIndexed(String type) throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("index", false).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", 123)
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
    }

    @Override
    public void doTestNoDocValues(String type) throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("doc_values", false).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", 123)
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
    }

    @Override
    public void doTestStore(String type) throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("store", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", 123)
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(123, storedField.numericValue().doubleValue(), 0d);
    }

    @Override
    public void doTestCoerce(String type) throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "123")
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("coerce", false).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper2 = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper2.mappingSource().toString());

        ThrowingRunnable runnable = () -> mapper2.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "123")
                .endObject()
                .bytes());
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    public void testIgnoreMalformed() throws Exception {
        for (String type : TYPES) {
            doTestIgnoreMalformed(type);
        }
    }

    private void doTestIgnoreMalformed(String type) throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ThrowingRunnable runnable = () -> mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "a")
                .endObject()
                .bytes());
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("For input string: \"a\""));

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("ignore_malformed", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper2 = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper2.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "a")
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testRejectNorms() throws IOException {
        // not supported as of 5.0
        for (String type : Arrays.asList("byte", "short", "integer", "long", "float", "double")) {
            DocumentMapperParser parser = createIndex("index-" + type).mapperService().documentMapperParser();
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", type)
                        .field("norms", random().nextBoolean())
                    .endObject()
                .endObject().endObject().endObject().string();
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> parser.parse("type", new CompressedXContent(mapping)));
            assertThat(e.getMessage(), containsString("Mapping definition for [foo] has unsupported parameters:  [norms"));
        }
    }

    @Override
    protected void doTestNullValue(String type) throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", type)
                        .endObject()
                    .endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .nullField("field")
                .endObject()
                .bytes());
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        Object missing;
        if (Arrays.asList("float", "double").contains(type)) {
            missing = 123d;
        } else {
            missing = 123L;
        }
        mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", type)
                            .field("null_value", missing)
                        .endObject()
                    .endObject()
                .endObject().endObject().string();

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .nullField("field")
                .endObject()
                .bytes());
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    public void testEmptyName() throws IOException {
        // after version 5
        for (String type : TYPES) {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("").field("type", type).endObject().endObject()
                .endObject().endObject().string();

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping))
            );
            assertThat(e.getMessage(), containsString("name cannot be empty string"));
        }
    }
}
