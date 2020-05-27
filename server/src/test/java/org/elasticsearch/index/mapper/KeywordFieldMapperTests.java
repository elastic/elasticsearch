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

import org.apache.lucene.analysis.MockLowerCaseFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.lucene.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class KeywordFieldMapperTests extends FieldMapperTestCase<KeywordFieldMapper.Builder> {

    @Override
    protected KeywordFieldMapper.Builder newBuilder() {
        return new KeywordFieldMapper.Builder("keyword");
    }

    /**
     * Creates a copy of the lowercase token filter which we use for testing merge errors.
     */
    public static class MockAnalysisPlugin extends Plugin implements AnalysisPlugin {
        @Override
        public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
            return singletonList(PreConfiguredTokenFilter.singleton("mock_other_lowercase", true, MockLowerCaseFilter::new));
        }

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
            return singletonMap("keyword", (indexSettings, environment, name, settings) ->
                TokenizerFactory.newFactory(name, () -> new MockTokenizer(MockTokenizer.KEYWORD, false)));
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, MockAnalysisPlugin.class);
    }

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test", Settings.builder()
                .put("index.analysis.normalizer.my_lowercase.type", "custom")
                .putList("index.analysis.normalizer.my_lowercase.filter", "lowercase")
                .put("index.analysis.normalizer.my_other_lowercase.type", "custom")
                .putList("index.analysis.normalizer.my_other_lowercase.filter", "mock_other_lowercase").build());
        parser = indexService.mapperService().documentMapperParser();
        addModifier("normalizer", false, (a, b) -> {
            a.normalizer(indexService.getIndexAnalyzers(), "my_lowercase");
        });
        addBooleanModifier("split_queries_on_whitespace", true, KeywordFieldMapper.Builder::splitQueriesOnWhitespace);
    }

    public void testDefaults() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "keyword").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "1234")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals(new BytesRef("1234"), fields[0].binaryValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        assertEquals(new BytesRef("1234"), fields[1].binaryValue());
        fieldType = fields[1].fieldType();
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.NONE));
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());

        // used by TermVectorsService
        assertArrayEquals(new String[] { "1234" }, TermVectorsService.getValues(doc.rootDoc().getFields("field")));
    }

    public void testIgnoreAbove() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "keyword").field("ignore_above", 5).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "elk")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "elasticsearch")
                        .endObject()),
                XContentType.JSON));

        fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testNullValue() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "keyword").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("field")
                        .endObject()),
                XContentType.JSON));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "keyword").field("null_value", "uri").endObject().endObject()
                .endObject().endObject());

        mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);

        doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("field")
                        .endObject()),
                XContentType.JSON));

        fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(new BytesRef("uri"), fields[0].binaryValue());
    }

    public void testEnableStore() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "keyword").field("store", true).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "1234")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertTrue(fields[0].fieldType().stored());
    }

    public void testDisableIndex() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "keyword").field("index", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "1234")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals(IndexOptions.NONE, fields[0].fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_SET, fields[0].fieldType().docValuesType());
    }

    public void testDisableDocValues() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "keyword").field("doc_values", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "1234")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());
    }

    public void testIndexOptions() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "keyword")
                .field("index_options", "freqs").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "1234")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(IndexOptions.DOCS_AND_FREQS, fields[0].fieldType().indexOptions());

        for (String indexOptions : Arrays.asList("positions", "offsets")) {
            final String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                    .startObject("properties").startObject("field").field("type", "keyword")
                    .field("index_options", indexOptions).endObject().endObject()
                    .endObject().endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> parser.parse("type", new CompressedXContent(mapping2)));
            assertEquals("The [keyword] field does not support positions, got [index_options]=" + indexOptions, e.getMessage());
        }
    }

    public void testBoost() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "keyword").field("boost", 2f).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());
    }

    public void testEnableNorms() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
                .startObject("field")
                    .field("type", "keyword")
                    .field("doc_values", false)
                    .field("norms", true)
                .endObject()
            .endObject()
        .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "1234")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertFalse(fields[0].fieldType().omitNorms());

        IndexableField[] fieldNamesFields = doc.rootDoc().getFields(FieldNamesFieldMapper.NAME);
        assertEquals(0, fieldNamesFields.length);
    }

    public void testCustomNormalizer() throws IOException {
        checkLowercaseNormalizer("my_lowercase");
    }

    public void testInBuiltNormalizer() throws IOException {
        checkLowercaseNormalizer("lowercase");
    }

    public void checkLowercaseNormalizer(String normalizerName) throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "keyword").field("normalizer", normalizerName).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "AbC")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals(new BytesRef("abc"), fields[0].binaryValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        assertEquals(new BytesRef("abc"), fields[1].binaryValue());
        fieldType = fields[1].fieldType();
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.NONE));
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());
    }

    public void testParsesKeywordNestedEmptyObjectStrict() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("field")
                .endObject()
            .endObject());
        MapperParsingException ex = expectThrows(MapperParsingException.class,
                () -> defaultMapper.parse(new SourceToParse("test", "1", source, XContentType.JSON)));
        assertEquals("failed to parse field [field] of type [keyword] in document with id '1'. " +
            "Preview of field's value: '{}'", ex.getMessage());
    }

    public void testParsesKeywordNestedListStrict() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
                .startArray("field")
                    .startObject()
                        .startArray("array_name")
                            .value("inner_field_first")
                            .value("inner_field_second")
                        .endArray()
                    .endObject()
                .endArray()
            .endObject());
        MapperParsingException ex = expectThrows(MapperParsingException.class,
                () -> defaultMapper.parse(new SourceToParse("test", "1", source, XContentType.JSON)));
        assertEquals("failed to parse field [field] of type [keyword] in document with id '1'. " +
            "Preview of field's value: '{array_name=[inner_field_first, inner_field_second]}'", ex.getMessage());
    }

    public void testParsesKeywordNullStrict() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("field")
                    .nullField("field_name")
                .endObject()
            .endObject());
        MapperParsingException ex = expectThrows(MapperParsingException.class,
                () -> defaultMapper.parse(new SourceToParse("test", "1", source, XContentType.JSON)));
        assertEquals("failed to parse field [field] of type [keyword] in document with id '1'. " +
            "Preview of field's value: '{field_name=null}'", ex.getMessage());
    }

    public void testUpdateNormalizer() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "keyword").field("normalizer", "my_lowercase").endObject().endObject()
                .endObject().endObject());
        indexService.mapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "keyword").field("normalizer", "my_other_lowercase").endObject().endObject()
                .endObject().endObject());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> indexService.mapperService().merge("type",
                        new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE));
        assertEquals(
                "Mapper for [field] conflicts with existing mapping:\n" +
                    "[mapper [field] has different [analyzer], mapper [field] has different [normalizer]]",
                e.getMessage());
    }

    public void testEmptyName() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject().endObject());

        // Empty name not allowed in index created after 5.0
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    public void testSplitQueriesOnWhitespace() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "keyword")
                    .endObject()
                    .startObject("field_with_normalizer")
                        .field("type", "keyword")
                        .field("normalizer", "my_lowercase")
                        .field("split_queries_on_whitespace", true)
                    .endObject()
                .endObject()
            .endObject().endObject());
        indexService.mapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        MappedFieldType fieldType = indexService.mapperService().fieldType("field");
        assertThat(fieldType, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        KeywordFieldMapper.KeywordFieldType ft = (KeywordFieldMapper.KeywordFieldType) fieldType;
        assertTokenStreamContents(ft.searchAnalyzer().analyzer().tokenStream("", "Hello World"), new String[] {"Hello World"});

        fieldType = indexService.mapperService().fieldType("field_with_normalizer");
        assertThat(fieldType, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        ft = (KeywordFieldMapper.KeywordFieldType) fieldType;
        assertThat(ft.searchAnalyzer().name(), equalTo("my_lowercase"));
        assertTokenStreamContents(ft.searchAnalyzer().analyzer().tokenStream("", "Hello World"), new String[] {"hello", "world"});

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "keyword")
                        .field("split_queries_on_whitespace", true)
                    .endObject()
                    .startObject("field_with_normalizer")
                        .field("type", "keyword")
                        .field("normalizer", "my_lowercase")
                        .field("split_queries_on_whitespace", false)
                    .endObject()
                .endObject()
            .endObject().endObject());
        indexService.mapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        fieldType = indexService.mapperService().fieldType("field");
        assertThat(fieldType, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        ft = (KeywordFieldMapper.KeywordFieldType) fieldType;
        assertTokenStreamContents(ft.searchAnalyzer().analyzer().tokenStream("", "Hello World"), new String[] {"Hello", "World"});

        fieldType = indexService.mapperService().fieldType("field_with_normalizer");
        assertThat(fieldType, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        ft = (KeywordFieldMapper.KeywordFieldType) fieldType;
        assertThat(ft.searchAnalyzer().name(), equalTo("my_lowercase"));
        assertTokenStreamContents(ft.searchAnalyzer().analyzer().tokenStream("", "Hello World"), new String[] {"hello world"});
    }

    public void testMeta() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "keyword")
                .field("meta", Collections.singletonMap("foo", "bar"))
                .endObject().endObject().endObject().endObject());

        DocumentMapper mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "keyword")
                .endObject().endObject().endObject().endObject());
        mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping2, mapper.mappingSource().toString());

        String mapping3 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "keyword")
                .field("meta", Collections.singletonMap("baz", "quux"))
                .endObject().endObject().endObject().endObject());
        mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }
}
