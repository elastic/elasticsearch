/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.string;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
 */
public class SimpleStringMappingTests extends ElasticsearchTestCase {

    private static Settings DOC_VALUES_SETTINGS = ImmutableSettings.builder().put(FieldDataType.FORMAT_KEY, FieldDataType.DOC_VALUES_FORMAT_VALUE).build();

    @Test
    public void testLimit() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("ignore_above", 5).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field"), notNullValue());

        doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "12345")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field"), notNullValue());

        doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "123456")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field"), nullValue());
    }

    @Test
    public void testDefaultsForAnalyzed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field").fieldType().omitNorms(), equalTo(false));
        assertThat(doc.rootDoc().getField("field").fieldType().indexOptions(), equalTo(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS));
        assertThat(doc.rootDoc().getField("field").fieldType().storeTermVectors(), equalTo(false));
        assertThat(doc.rootDoc().getField("field").fieldType().storeTermVectorOffsets(), equalTo(false));
        assertThat(doc.rootDoc().getField("field").fieldType().storeTermVectorPositions(), equalTo(false));
        assertThat(doc.rootDoc().getField("field").fieldType().storeTermVectorPayloads(), equalTo(false));
    }

    @Test
    public void testDefaultsForNotAnalyzed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("index", "not_analyzed").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field").fieldType().omitNorms(), equalTo(true));
        assertThat(doc.rootDoc().getField("field").fieldType().indexOptions(), equalTo(FieldInfo.IndexOptions.DOCS_ONLY));
        assertThat(doc.rootDoc().getField("field").fieldType().storeTermVectors(), equalTo(false));
        assertThat(doc.rootDoc().getField("field").fieldType().storeTermVectorOffsets(), equalTo(false));
        assertThat(doc.rootDoc().getField("field").fieldType().storeTermVectorPositions(), equalTo(false));
        assertThat(doc.rootDoc().getField("field").fieldType().storeTermVectorPayloads(), equalTo(false));

        // now test it explicitly set

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("index", "not_analyzed").field("omit_norms", false).field("index_options", "freqs").endObject().endObject()
                .endObject().endObject().string();

        defaultMapper = MapperTestUtils.newParser().parse(mapping);

        doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field").fieldType().omitNorms(), equalTo(false));
        assertThat(doc.rootDoc().getField("field").fieldType().indexOptions(), equalTo(FieldInfo.IndexOptions.DOCS_AND_FREQS));
        assertThat(doc.rootDoc().getField("field").fieldType().storeTermVectors(), equalTo(false));
        assertThat(doc.rootDoc().getField("field").fieldType().storeTermVectorOffsets(), equalTo(false));
        assertThat(doc.rootDoc().getField("field").fieldType().storeTermVectorPositions(), equalTo(false));
        assertThat(doc.rootDoc().getField("field").fieldType().storeTermVectorPayloads(), equalTo(false));
    }

    @Test
    public void testTermVectors() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1")
                    .field("type", "string")
                    .field("term_vector", "no")
                .endObject()
                .startObject("field2")
                    .field("type", "string")
                    .field("term_vector", "yes")
                .endObject()
                .startObject("field3")
                    .field("type", "string")
                    .field("term_vector", "with_offsets")
                .endObject()
                .startObject("field4")
                    .field("type", "string")
                    .field("term_vector", "with_positions")
                .endObject()
                .startObject("field5")
                    .field("type", "string")
                    .field("term_vector", "with_positions_offsets")
                .endObject()
                .startObject("field6")
                    .field("type", "string")
                    .field("term_vector", "with_positions_offsets_payloads")
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "1234")
                .field("field2", "1234")
                .field("field3", "1234")
                .field("field4", "1234")
                .field("field5", "1234")
                .field("field6", "1234")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field1").fieldType().storeTermVectors(), equalTo(false));
        assertThat(doc.rootDoc().getField("field1").fieldType().storeTermVectorOffsets(), equalTo(false));
        assertThat(doc.rootDoc().getField("field1").fieldType().storeTermVectorPositions(), equalTo(false));
        assertThat(doc.rootDoc().getField("field1").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field2").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field2").fieldType().storeTermVectorOffsets(), equalTo(false));
        assertThat(doc.rootDoc().getField("field2").fieldType().storeTermVectorPositions(), equalTo(false));
        assertThat(doc.rootDoc().getField("field2").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field3").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field3").fieldType().storeTermVectorOffsets(), equalTo(true));
        assertThat(doc.rootDoc().getField("field3").fieldType().storeTermVectorPositions(), equalTo(false));
        assertThat(doc.rootDoc().getField("field3").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field4").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field4").fieldType().storeTermVectorOffsets(), equalTo(false));
        assertThat(doc.rootDoc().getField("field4").fieldType().storeTermVectorPositions(), equalTo(true));
        assertThat(doc.rootDoc().getField("field4").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field5").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field5").fieldType().storeTermVectorOffsets(), equalTo(true));
        assertThat(doc.rootDoc().getField("field5").fieldType().storeTermVectorPositions(), equalTo(true));
        assertThat(doc.rootDoc().getField("field5").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field6").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field6").fieldType().storeTermVectorOffsets(), equalTo(true));
        assertThat(doc.rootDoc().getField("field6").fieldType().storeTermVectorPositions(), equalTo(true));
        assertThat(doc.rootDoc().getField("field6").fieldType().storeTermVectorPayloads(), equalTo(true));
    }

    public void testDocValues() throws Exception {
        // doc values only work on non-analyzed content
        final BuilderContext ctx = new BuilderContext(null, new ContentPath(1));
        try {
            new StringFieldMapper.Builder("anything").fieldDataSettings(DOC_VALUES_SETTINGS).build(ctx);
            fail();
        } catch (Exception e) { /* OK */ }
        new StringFieldMapper.Builder("anything").tokenized(false).fieldDataSettings(DOC_VALUES_SETTINGS).build(ctx);
        new StringFieldMapper.Builder("anything").index(false).fieldDataSettings(DOC_VALUES_SETTINGS).build(ctx);

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("str1")
                    .field("type", "string")
                    .startObject("fielddata")
                        .field("format", "fst")
                    .endObject()
                .endObject()
                .startObject("str2")
                    .field("type", "string")
                    .field("index", "not_analyzed")
                    .startObject("fielddata")
                        .field("format", "doc_values")
                    .endObject()
                .endObject()
                .startObject("int")
                    .field("type", "integer")
                    .startObject("fielddata")
                        .field("format", "doc_values")
                    .endObject()
                .endObject()
                .startObject("double")
                    .field("type", "double")
                    .startObject("fielddata")
                        .field("format", "doc_values")
                    .endObject()
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        ParsedDocument parsedDoc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("str1", "1234")
                .field("str2", "1234")
                .field("int", "1234")
                .field("double", "1234")
                .endObject()
                .bytes());
        final Document doc = parsedDoc.rootDoc();
        assertEquals(null, docValuesType(doc, "str1"));
        assertEquals(DocValuesType.SORTED_SET, docValuesType(doc, "str2"));
        assertEquals(DocValuesType.SORTED_SET, docValuesType(doc, "int"));
        assertEquals(DocValuesType.SORTED_SET, docValuesType(doc, "double"));
    }

    private static DocValuesType docValuesType(Document document, String fieldName) {
        for (IndexableField field : document.getFields(fieldName)) {
            if (field.fieldType().docValueType() != null) {
                return field.fieldType().docValueType();
            }
        }
        return null;
    }

}
