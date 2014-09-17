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

package org.elasticsearch.index.mapper.string;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.DocumentMapper.MergeFlags;
import org.elasticsearch.index.mapper.DocumentMapper.MergeResult;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.*;

/**
 */
public class SimpleStringMappingTests extends ElasticsearchSingleNodeTest {

    private static Settings DOC_VALUES_SETTINGS = ImmutableSettings.builder().put(FieldDataType.FORMAT_KEY, FieldDataType.DOC_VALUES_FORMAT_VALUE).build();

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void before() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    @Test
    public void testLimit() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("ignore_above", 5).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse(mapping);

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

    private void assertDefaultAnalyzedFieldType(IndexableFieldType fieldType) {
        assertThat(fieldType.omitNorms(), equalTo(false));
        assertThat(fieldType.indexOptions(), equalTo(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
    }

    private void assertEquals(IndexableFieldType ft1, IndexableFieldType ft2) {
        assertEquals(ft1.indexed(), ft2.indexed());
        assertEquals(ft1.tokenized(), ft2.tokenized());
        assertEquals(ft1.omitNorms(), ft2.omitNorms());
        assertEquals(ft1.indexOptions(), ft2.indexOptions());
        assertEquals(ft1.storeTermVectors(), ft2.storeTermVectors());
        assertEquals(ft1.docValueType(), ft2.docValueType());
    }

    private void assertParseIdemPotent(IndexableFieldType expected, DocumentMapper mapper) throws Exception {
        String mapping = mapper.toXContent(XContentFactory.jsonBuilder().startObject(), new ToXContent.MapParams(ImmutableMap.<String, String>of())).endObject().string();
        mapper = parser.parse(mapping);
        ParsedDocument doc = mapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "2345")
                .endObject()
                .bytes());
        assertEquals(expected, doc.rootDoc().getField("field").fieldType());
    }

    @Test
    public void testDefaultsForAnalyzed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        IndexableFieldType fieldType = doc.rootDoc().getField("field").fieldType();
        assertDefaultAnalyzedFieldType(fieldType);
        assertParseIdemPotent(fieldType, defaultMapper);
    }

    @Test
    public void testDefaultsForNotAnalyzed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("index", "not_analyzed").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        IndexableFieldType fieldType = doc.rootDoc().getField("field").fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertThat(fieldType.indexOptions(), equalTo(FieldInfo.IndexOptions.DOCS_ONLY));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertParseIdemPotent(fieldType, defaultMapper);

        // now test it explicitly set

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("index", "not_analyzed").startObject("norms").field("enabled", true).endObject().field("index_options", "freqs").endObject().endObject()
                .endObject().endObject().string();

        defaultMapper = parser.parse(mapping);

        doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        fieldType = doc.rootDoc().getField("field").fieldType();
        assertThat(fieldType.omitNorms(), equalTo(false));
        assertThat(fieldType.indexOptions(), equalTo(FieldInfo.IndexOptions.DOCS_AND_FREQS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertParseIdemPotent(fieldType, defaultMapper);

        // also test the deprecated omit_norms

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("index", "not_analyzed").field("omit_norms", false).endObject().endObject()
                .endObject().endObject().string();

        defaultMapper = parser.parse(mapping);

        doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        fieldType = doc.rootDoc().getField("field").fieldType();
        assertThat(fieldType.omitNorms(), equalTo(false));
        assertParseIdemPotent(fieldType, defaultMapper);
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

        DocumentMapper defaultMapper = parser.parse(mapping);

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
        final BuilderContext ctx = new BuilderContext(indexService.settingsService().getSettings(), new ContentPath(1));
        try {
            new StringFieldMapper.Builder("anything").fieldDataSettings(DOC_VALUES_SETTINGS).build(ctx);
            fail();
        } catch (Exception e) { /* OK */ }
        new StringFieldMapper.Builder("anything").tokenized(false).fieldDataSettings(DOC_VALUES_SETTINGS).build(ctx);
        new StringFieldMapper.Builder("anything").index(false).fieldDataSettings(DOC_VALUES_SETTINGS).build(ctx);

        assertFalse(new StringFieldMapper.Builder("anything").index(false).build(ctx).hasDocValues());
        assertTrue(new StringFieldMapper.Builder("anything").index(false).fieldDataSettings(DOC_VALUES_SETTINGS).build(ctx).hasDocValues());
        assertTrue(new StringFieldMapper.Builder("anything").index(false).docValues(true).build(ctx).hasDocValues());

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
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse(mapping);

        ParsedDocument parsedDoc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("str1", "1234")
                .field("str2", "1234")
                .endObject()
                .bytes());
        final Document doc = parsedDoc.rootDoc();
        assertEquals(null, docValuesType(doc, "str1"));
        assertEquals(DocValuesType.SORTED_SET, docValuesType(doc, "str2"));
    }

    public static DocValuesType docValuesType(Document document, String fieldName) {
        for (IndexableField field : document.getFields(fieldName)) {
            if (field.fieldType().docValueType() != null) {
                return field.fieldType().docValueType();
            }
        }
        return null;
    }

    @Test
    public void testDisableNorms() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        IndexableFieldType fieldType = doc.rootDoc().getField("field").fieldType();
        assertEquals(false, fieldType.omitNorms());

        String updatedMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").startObject("norms").field("enabled", false).endObject()
                .endObject().endObject().endObject().endObject().string();
        MergeResult mergeResult = defaultMapper.merge(parser.parse(updatedMapping), MergeFlags.mergeFlags().simulate(false));
        assertFalse(Arrays.toString(mergeResult.conflicts()), mergeResult.hasConflicts());

        doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        fieldType = doc.rootDoc().getField("field").fieldType();
        assertEquals(true, fieldType.omitNorms());

        updatedMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").startObject("norms").field("enabled", true).endObject()
                .endObject().endObject().endObject().endObject().string();
        mergeResult = defaultMapper.merge(parser.parse(updatedMapping), MergeFlags.mergeFlags());
        assertTrue(mergeResult.hasConflicts());
        assertEquals(1, mergeResult.conflicts().length);
        assertTrue(mergeResult.conflicts()[0].contains("cannot enable norms"));
    }

}
