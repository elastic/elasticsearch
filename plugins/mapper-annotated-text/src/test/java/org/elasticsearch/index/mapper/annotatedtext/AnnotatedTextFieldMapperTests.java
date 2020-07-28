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

package org.elasticsearch.index.mapper.annotatedtext;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugin.mapper.AnnotatedTextPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class AnnotatedTextFieldMapperTests extends ESSingleNodeTestCase {

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void setup() {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.my_stop_analyzer.tokenizer", "standard")
            .put("index.analysis.analyzer.my_stop_analyzer.filter", "stop")
            .build();
        indexService = createIndex("test", settings);
        parser = indexService.mapperService().documentMapperParser();
    }



    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> classpathPlugins = new ArrayList<>();
        classpathPlugins.add(AnnotatedTextPlugin.class);
        return classpathPlugins;
    }



    protected String getFieldType() {
        return "annotated_text";
    }

    public void testAnnotationInjection() throws IOException {

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", getFieldType()).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = indexService.mapperService().merge("type",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        // Use example of typed and untyped annotations
        String annotatedText = "He paid [Stormy Daniels](Stephanie+Clifford&Payee) hush money";
        SourceToParse sourceToParse = new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", annotatedText)
                        .endObject()),
            XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);

        assertEquals(annotatedText, fields[0].stringValue());

        IndexShard shard = indexService.getShard(0);
        shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL,
            sourceToParse, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            LeafReader leaf = searcher.getDirectoryReader().leaves().get(0).reader();
            TermsEnum terms = leaf.terms("field").iterator();

            assertTrue(terms.seekExact(new BytesRef("stormy")));
            PostingsEnum postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(2, postings.nextPosition());

            assertTrue(terms.seekExact(new BytesRef("Stephanie Clifford")));
            postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(2, postings.nextPosition());

            assertTrue(terms.seekExact(new BytesRef("Payee")));
            postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(2, postings.nextPosition());


            assertTrue(terms.seekExact(new BytesRef("hush")));
            postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(4, postings.nextPosition());

        }
    }

    public void testToleranceForBadAnnotationMarkup() throws IOException {

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", getFieldType()).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = indexService.mapperService().merge("type",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        String annotatedText = "foo [bar](MissingEndBracket baz";
        SourceToParse sourceToParse = new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", annotatedText)
                        .endObject()),
            XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);

        assertEquals(annotatedText, fields[0].stringValue());

        IndexShard shard = indexService.getShard(0);
        shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL,
            sourceToParse, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            LeafReader leaf = searcher.getDirectoryReader().leaves().get(0).reader();
            TermsEnum terms = leaf.terms("field").iterator();

            assertTrue(terms.seekExact(new BytesRef("foo")));
            PostingsEnum postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(0, postings.nextPosition());

            assertTrue(terms.seekExact(new BytesRef("bar")));
            postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(1, postings.nextPosition());

            assertFalse(terms.seekExact(new BytesRef("MissingEndBracket")));
            // Bad markup means value is treated as plain text and fed through tokenisation
            assertTrue(terms.seekExact(new BytesRef("missingendbracket")));

        }
    }

    public void testAgainstTermVectorsAPI() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("tvfield").field("type", getFieldType())
                .field("term_vector", "with_positions_offsets_payloads")
                .endObject().endObject()
                .endObject().endObject());
        indexService.mapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);


        int max = between(3, 10);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < max; i++) {
            bulk.add(client().prepareIndex("test").setId(Integer.toString(i))
                    .setSource("tvfield", "the quick [brown](Color) fox jumped over the lazy dog"));
        }
        bulk.get();

        TermVectorsRequest request = new TermVectorsRequest("test", "0").termStatistics(true);

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        assertThat(shard, notNullValue());
        TermVectorsResponse response = TermVectorsService.getTermVectors(shard, request);
        assertEquals(1, response.getFields().size());

        Terms terms = response.getFields().terms("tvfield");
        TermsEnum iterator = terms.iterator();
        BytesRef term;
        Set<String> foundTerms = new HashSet<>();
        while ((term = iterator.next()) != null) {
            foundTerms.add(term.utf8ToString());
        }
        //Check we have both text and annotation tokens
        assertTrue(foundTerms.contains("brown"));
        assertTrue(foundTerms.contains("Color"));
        assertTrue(foundTerms.contains("fox"));

    }

    // ===== Code below copied from TextFieldMapperTests ========

    public void testDefaults() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", getFieldType()).endObject().endObject()
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

        assertEquals("1234", fields[0].stringValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(false));
        assertTrue(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());
    }

    public void testEnableStore() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", getFieldType()).field("store", true).endObject().endObject()
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
        assertTrue(fields[0].fieldType().stored());
    }

    public void testDisableNorms() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", getFieldType())
                    .field("norms", false)
                .endObject().endObject()
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
        assertTrue(fields[0].fieldType().omitNorms());
    }

    public void testIndexOptions() throws IOException {
        Map<String, IndexOptions> supportedOptions = new HashMap<>();
        supportedOptions.put("docs", IndexOptions.DOCS);
        supportedOptions.put("freqs", IndexOptions.DOCS_AND_FREQS);
        supportedOptions.put("positions", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        supportedOptions.put("offsets", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties");
        for (String option : supportedOptions.keySet()) {
            mappingBuilder.startObject(option).field("type", getFieldType()).field("index_options", option).endObject();
        }
        String mapping = Strings.toString(mappingBuilder.endObject().endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        XContentBuilder jsonDoc = XContentFactory.jsonBuilder().startObject();
        for (String option : supportedOptions.keySet()) {
            jsonDoc.field(option, "1234");
        }
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(jsonDoc.endObject()),
                XContentType.JSON));

        for (Map.Entry<String, IndexOptions> entry : supportedOptions.entrySet()) {
            String field = entry.getKey();
            IndexOptions options = entry.getValue();
            IndexableField[] fields = doc.rootDoc().getFields(field);
            assertEquals(1, fields.length);
            assertEquals(options, fields[0].fieldType().indexOptions());
        }
    }

    public void testDefaultPositionIncrementGap() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", getFieldType()).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = indexService.mapperService().merge("type",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        assertEquals(mapping, mapper.mappingSource().toString());

        SourceToParse sourceToParse = new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .array("field", new String[] {"a", "b"})
                        .endObject()),
            XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals("a", fields[0].stringValue());
        assertEquals("b", fields[1].stringValue());

        IndexShard shard = indexService.getShard(0);
        shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL,
            sourceToParse, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            LeafReader leaf = searcher.getDirectoryReader().leaves().get(0).reader();
            TermsEnum terms = leaf.terms("field").iterator();
            assertTrue(terms.seekExact(new BytesRef("b")));
            PostingsEnum postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(TextFieldMapper.Defaults.POSITION_INCREMENT_GAP + 1, postings.nextPosition());
        }
    }

    public void testPositionIncrementGap() throws IOException {
        final int positionIncrementGap = randomIntBetween(1, 1000);
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", getFieldType())
                    .field("position_increment_gap", positionIncrementGap)
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = indexService.mapperService().merge("type",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        assertEquals(mapping, mapper.mappingSource().toString());

        SourceToParse sourceToParse = new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .array("field", new String[]{"a", "b"})
                        .endObject()),
            XContentType.JSON);
        ParsedDocument doc = mapper.parse(sourceToParse);

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals("a", fields[0].stringValue());
        assertEquals("b", fields[1].stringValue());

        IndexShard shard = indexService.getShard(0);
        shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL,
            sourceToParse, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            LeafReader leaf = searcher.getDirectoryReader().leaves().get(0).reader();
            TermsEnum terms = leaf.terms("field").iterator();
            assertTrue(terms.seekExact(new BytesRef("b")));
            PostingsEnum postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(positionIncrementGap + 1, postings.nextPosition());
        }
    }

    public void testSearchAnalyzerSerialization() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", getFieldType())
                        .field("analyzer", "standard")
                        .field("search_analyzer", "keyword")
                    .endObject()
                .endObject().endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        // special case: default index analyzer
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", getFieldType())
                        .field("analyzer", "default")
                        .field("search_analyzer", "keyword")
                    .endObject()
                .endObject().endObject().endObject());

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", getFieldType())
            .field("analyzer", "keyword")
            .endObject()
            .endObject().endObject().endObject());

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        // special case: default search analyzer
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", getFieldType())
            .field("analyzer", "keyword")
            .field("search_analyzer", "default")
            .endObject()
            .endObject().endObject().endObject());

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", getFieldType())
            .field("analyzer", "keyword")
            .endObject()
            .endObject().endObject().endObject());
        mapper = parser.parse("type", new CompressedXContent(mapping));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        mapper.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true")));
        builder.endObject();

        String mappingString = Strings.toString(builder);
        assertTrue(mappingString.contains("analyzer"));
        assertTrue(mappingString.contains("search_analyzer"));
        assertTrue(mappingString.contains("search_quote_analyzer"));
    }

    public void testSearchQuoteAnalyzerSerialization() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", getFieldType())
                        .field("analyzer", "standard")
                        .field("search_analyzer", "standard")
                        .field("search_quote_analyzer", "keyword")
                    .endObject()
                .endObject().endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        // special case: default index/search analyzer
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", getFieldType())
                        .field("analyzer", "default")
                        .field("search_analyzer", "default")
                        .field("search_quote_analyzer", "keyword")
                    .endObject()
                .endObject().endObject().endObject());

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());
    }

    public void testTermVectors() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1")
                    .field("type", getFieldType())
                    .field("term_vector", "no")
                .endObject()
                .startObject("field2")
                    .field("type", getFieldType())
                    .field("term_vector", "yes")
                .endObject()
                .startObject("field3")
                    .field("type", getFieldType())
                    .field("term_vector", "with_offsets")
                .endObject()
                .startObject("field4")
                    .field("type", getFieldType())
                    .field("term_vector", "with_positions")
                .endObject()
                .startObject("field5")
                    .field("type", getFieldType())
                    .field("term_vector", "with_positions_offsets")
                .endObject()
                .startObject("field6")
                    .field("type", getFieldType())
                    .field("term_vector", "with_positions_offsets_payloads")
                .endObject()
                .endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field1", "1234")
                        .field("field2", "1234")
                        .field("field3", "1234")
                        .field("field4", "1234")
                        .field("field5", "1234")
                        .field("field6", "1234")
                        .endObject()),
                XContentType.JSON));

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

    public void testNullConfigValuesFail() throws MapperParsingException, IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", getFieldType())
                            .field("analyzer", (String) null)
                        .endObject()
                    .endObject()
                .endObject().endObject());

        Exception e = expectThrows(MapperParsingException.class, () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("[analyzer] must not have a [null] value", e.getMessage());
    }

    public void testNotIndexedField() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
            .field("type", getFieldType())
            .field("index", false)
            .endObject().endObject().endObject().endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("[annotated_text] fields must be indexed", e.getMessage());
    }

    public void testAnalyzedFieldPositionIncrementWithoutPositions() throws IOException {
        for (String indexOptions : Arrays.asList("docs", "freqs")) {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", getFieldType())
                .field("index_options", indexOptions)
                .field("position_increment_gap", 10)
                .endObject().endObject().endObject().endObject());

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping)));
            assertEquals("Cannot set position_increment_gap on field [field] without positions enabled", e.getMessage());
        }
    }

    public void testEmptyName() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("")
                            .field("type", getFieldType())
                        .endObject()
                    .endObject()
                .endObject().endObject());

        // Empty name not allowed in index created after 5.0
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    public void testParseSourceValue() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        FieldMapper fieldMapper = new AnnotatedTextFieldMapper.Builder("field")
            .indexAnalyzer(indexService.getIndexAnalyzers().getDefaultIndexAnalyzer())
            .searchAnalyzer(indexService.getIndexAnalyzers().getDefaultSearchAnalyzer())
            .searchQuoteAnalyzer(indexService.getIndexAnalyzers().getDefaultSearchQuoteAnalyzer())
            .build(context);
        AnnotatedTextFieldMapper mapper = (AnnotatedTextFieldMapper) fieldMapper;

        assertEquals("value", mapper.parseSourceValue("value", null));
        assertEquals("42", mapper.parseSourceValue(42L, null));
        assertEquals("true", mapper.parseSourceValue(true, null));
    }
}
