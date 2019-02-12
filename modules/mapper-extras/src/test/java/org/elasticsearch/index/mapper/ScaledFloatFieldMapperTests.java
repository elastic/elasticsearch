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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ScaledFloatFieldMapperTests extends ESSingleNodeTestCase {

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, MapperExtrasPlugin.class);
    }

    public void testDefaults() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "scaled_float")
                .field("scaling_factor", 10.0).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 123)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDataDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(1230, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1230, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testMissingScalingFactor() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "scaled_float").endObject().endObject()
                .endObject().endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("Field [field] misses required parameter [scaling_factor]", e.getMessage());
    }

    public void testIllegalScalingFactor() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "scaled_float")
                .field("scaling_factor", -1).endObject().endObject()
                .endObject().endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("[scaling_factor] must be a positive number, got [-1.0]", e.getMessage());
    }

    public void testNotIndexed() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "scaled_float")
                .field("index", false).field("scaling_factor", 10.0).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 123)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1230, dvField.numericValue().longValue());
    }

    public void testNoDocValues() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "scaled_float")
                .field("doc_values", false).field("scaling_factor", 10.0).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 123)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDataDimensionCount());
        assertEquals(1230, pointField.numericValue().longValue());
    }

    public void testStore() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "scaled_float")
                .field("store", true).field("scaling_factor", 10.0).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 123)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDataDimensionCount());
        assertEquals(1230, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(1230, storedField.numericValue().longValue());
    }

    public void testCoerce() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "scaled_float")
                .field("scaling_factor", 10.0).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "123")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDataDimensionCount());
        assertEquals(1230, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "scaled_float")
                .field("scaling_factor", 10.0).field("coerce", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper2 = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper2.mappingSource().toString());

        ThrowingRunnable runnable = () -> mapper2.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "123")
                        .endObject()),
                XContentType.JSON));
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    public void testIgnoreMalformed() throws Exception {
        doTestIgnoreMalformed("a", "For input string: \"a\"");

        List<String> values = Arrays.asList("NaN", "Infinity", "-Infinity");
        for (String value : values) {
            doTestIgnoreMalformed(value, "[scaled_float] only supports finite values, but got [" + value + "]");
        }
    }

    private void doTestIgnoreMalformed(String value, String exceptionMessageContains) throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", "scaled_float")
            .field("scaling_factor", 10.0).endObject().endObject()
            .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ThrowingRunnable runnable = () -> mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", value)
                        .endObject()),
            XContentType.JSON));
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString(exceptionMessageContains));

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", "scaled_float")
            .field("scaling_factor", 10.0).field("ignore_malformed", true).endObject().endObject()
            .endObject().endObject());

        DocumentMapper mapper2 = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper2.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", value)
                        .endObject()),
            XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testNullValue() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "scaled_float")
                            .field("scaling_factor", 10.0)
                        .endObject()
                    .endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("field")
                        .endObject()),
                XContentType.JSON));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "scaled_float")
                            .field("scaling_factor", 10.0)
                            .field("null_value", 2.5)
                        .endObject()
                    .endObject()
                .endObject().endObject());

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("field")
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDataDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(25, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    public void testEmptyName() throws IOException {
        // after 5.x
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("")
            .field("type", "scaled_float")
            .field("scaling_factor", 10.0).endObject().endObject()
            .endObject().endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    /**
     * `index_options` was deprecated and is rejected as of 7.0
     */
    public void testRejectIndexOptions() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
                .startObject("foo")
                    .field("type", "scaled_float")
                .field("index_options", randomFrom(new String[] { "docs", "freqs", "positions", "offsets" }))
                .endObject()
            .endObject().endObject().endObject());
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> parser.parse("type", new CompressedXContent(mapping)));
        assertThat(e.getMessage(), containsString("index_options not allowed in field [foo] of type [scaled_float]"));
    }
}
