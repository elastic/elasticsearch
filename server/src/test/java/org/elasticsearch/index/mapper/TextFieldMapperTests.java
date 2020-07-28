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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockSynonymAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class TextFieldMapperTests extends FieldMapperTestCase<TextFieldMapper.Builder> {

    @Override
    protected TextFieldMapper.Builder newBuilder() {
        return new TextFieldMapper.Builder("text")
            .indexAnalyzer(new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()))
            .searchAnalyzer(new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));
    }

    @Before
    public void addModifiers() {
        addBooleanModifier("fielddata", true, TextFieldMapper.Builder::fielddata);
        addModifier("fielddata_frequency_filter.min", true, (a, b) -> {
            a.fielddataFrequencyFilter(1, 10, 10);
            a.fielddataFrequencyFilter(2, 10, 10);
        });
        addModifier("fielddata_frequency_filter.max", true, (a, b) -> {
            a.fielddataFrequencyFilter(1, 10, 10);
            a.fielddataFrequencyFilter(1, 12, 10);
        });
        addModifier("fielddata_frequency_filter.min_segment_size", true, (a, b) -> {
            a.fielddataFrequencyFilter(1, 10, 10);
            a.fielddataFrequencyFilter(1, 10, 11);
        });
        addModifier("index_phrases", false, (a, b) -> {
            a.indexPhrases(true);
            b.indexPhrases(false);
        });
        addModifier("index_prefixes", false, (a, b) -> {
            a.indexPrefixes(2, 4);
        });
        addModifier("index_options", false, (a, b) -> {
            a.indexOptions(IndexOptions.DOCS_AND_FREQS);
            b.indexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        });
        addModifier("similarity", false, (a, b) -> {
            a.similarity(new SimilarityProvider("BM25", new BM25Similarity()));
            b.similarity(new SimilarityProvider("boolean", new BooleanSimilarity()));
        });
    }

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("doc_values");
    }

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void setup() {
        Settings settings = Settings.builder()
            // Stop filter remains in server as it is part of lucene-core
            .put("index.analysis.analyzer.my_stop_analyzer.tokenizer", "standard")
            .put("index.analysis.analyzer.my_stop_analyzer.filter", "stop")
            .build();
        indexService = createIndex("test", settings);
        parser = indexService.mapperService().documentMapperParser();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testDefaults() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "text").endObject().endObject()
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
                .startObject("properties").startObject("field").field("type", "text").field("store", true).endObject().endObject()
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

    public void testDisableIndex() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "text").field("index", false).endObject().endObject()
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
        assertEquals(0, fields.length);
    }

    public void testDisableNorms() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
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
            mappingBuilder.startObject(option).field("type", "text").field("index_options", option).endObject();
        }
        String mapping = Strings.toString(mappingBuilder.endObject().endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        String serialized = Strings.toString(mapper);
        assertThat(serialized, containsString("\"offsets\":{\"type\":\"text\",\"index_options\":\"offsets\"}"));
        assertThat(serialized, containsString("\"freqs\":{\"type\":\"text\",\"index_options\":\"freqs\"}"));
        assertThat(serialized, containsString("\"docs\":{\"type\":\"text\",\"index_options\":\"docs\"}"));

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
                .startObject("properties").startObject("field").field("type", "text").endObject().endObject()
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
                    .field("type", "text")
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
                        .field("type", "text")
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
                        .field("type", "text")
                        .field("analyzer", "default")
                        .field("search_analyzer", "keyword")
                    .endObject()
                .endObject().endObject().endObject());

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("analyzer", "keyword")
            .endObject()
            .endObject().endObject().endObject());

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        // special case: default search analyzer
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("analyzer", "keyword")
            .field("search_analyzer", "default")
            .endObject()
            .endObject().endObject().endObject());

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
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
                        .field("type", "text")
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
                        .field("type", "text")
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
                    .field("type", "text")
                    .field("term_vector", "no")
                .endObject()
                .startObject("field2")
                    .field("type", "text")
                    .field("term_vector", "yes")
                .endObject()
                .startObject("field3")
                    .field("type", "text")
                    .field("term_vector", "with_offsets")
                .endObject()
                .startObject("field4")
                    .field("type", "text")
                    .field("term_vector", "with_positions")
                .endObject()
                .startObject("field5")
                    .field("type", "text")
                    .field("term_vector", "with_positions_offsets")
                .endObject()
                .startObject("field6")
                    .field("type", "text")
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

    public void testEagerGlobalOrdinals() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
                    .field("eager_global_ordinals", true)
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        assertTrue(fieldMapper.fieldType().eagerGlobalOrdinals());
    }

    public void testFielddata() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper disabledMapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, disabledMapper.mappingSource().toString());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            FieldMapper fieldMapper = (FieldMapper) disabledMapper.mappers().getMapper("field");
            fieldMapper.fieldType().fielddataBuilder("test");
        });
        assertThat(e.getMessage(), containsString("Text fields are not optimised for operations that require per-document field data"));

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
                    .field("fielddata", true)
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper enabledMapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, enabledMapper.mappingSource().toString());

        FieldMapper enabledFieldMapper = (FieldMapper) enabledMapper.mappers().getMapper("field");
        enabledFieldMapper.fieldType().fielddataBuilder("test"); // no exception this time

        String illegalMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
                    .field("index", false)
                    .field("fielddata", true)
                .endObject().endObject()
                .endObject().endObject());
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(illegalMapping)));
        assertThat(ex.getMessage(), containsString("Cannot enable fielddata on a [text] field that is not indexed"));
    }

    public void testFrequencyFilter() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
                    .field("fielddata", true)
                    .startObject("fielddata_frequency_filter")
                        .field("min", 2d)
                        .field("min_segment_size", 1000)
                    .endObject()
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());
        TextFieldMapper fieldMapper = (TextFieldMapper) mapper.mappers().getMapper("field");
        TextFieldType fieldType = fieldMapper.fieldType();

        assertThat(fieldType.fielddataMinFrequency(), equalTo(2d));
        assertThat(fieldType.fielddataMaxFrequency(), equalTo((double) Integer.MAX_VALUE));
        assertThat(fieldType.fielddataMinSegmentSize(), equalTo(1000));
    }

    public void testNullConfigValuesFail() throws MapperParsingException, IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "text")
                            .field("analyzer", (String) null)
                        .endObject()
                    .endObject()
                .endObject().endObject());

        Exception e = expectThrows(MapperParsingException.class, () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("[analyzer] must not have a [null] value", e.getMessage());
    }

    public void testNotIndexedFieldPositionIncrement() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
            .field("type", "text")
            .field("index", false)
            .field("position_increment_gap", 10)
            .endObject().endObject().endObject().endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("Cannot set position_increment_gap on field [field] without positions enabled", e.getMessage());
    }

    public void testAnalyzedFieldPositionIncrementWithoutPositions() throws IOException {
        for (String indexOptions : Arrays.asList("docs", "freqs")) {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
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
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject().endObject());

        // Empty name not allowed in index created after 5.0
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    public void testIndexPrefixIndexTypes() throws IOException {
        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .startObject("index_prefixes").endObject()
                .field("index_options", "offsets")
                .endObject().endObject().endObject().endObject());

            DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

            FieldMapper prefix = (FieldMapper) mapper.mappers().getMapper("field._index_prefix");
            assertEquals(prefix.name(), "field._index_prefix");
            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, prefix.fieldType.indexOptions());
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .startObject("index_prefixes").endObject()
                .field("index_options", "freqs")
                .endObject().endObject().endObject().endObject());

            DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

            FieldMapper prefix = (FieldMapper) mapper.mappers().getMapper("field._index_prefix");
            FieldType ft = prefix.fieldType;
            assertEquals(prefix.name(), "field._index_prefix");
            assertEquals(IndexOptions.DOCS, ft.indexOptions());
            assertFalse(ft.storeTermVectors());
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .startObject("index_prefixes").endObject()
                .field("index_options", "positions")
                .endObject().endObject().endObject().endObject());

            DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

            FieldMapper prefix = (FieldMapper) mapper.mappers().getMapper("field._index_prefix");
            FieldType ft = prefix.fieldType;
            assertEquals(prefix.name(), "field._index_prefix");
            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, ft.indexOptions());
            assertFalse(ft.storeTermVectors());
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .startObject("index_prefixes").endObject()
                .field("term_vector", "with_positions_offsets")
                .endObject().endObject().endObject().endObject());

            DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

            FieldMapper prefix = (FieldMapper) mapper.mappers().getMapper("field._index_prefix");
            FieldType ft = prefix.fieldType;
            assertEquals(prefix.name(), "field._index_prefix");
            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, ft.indexOptions());
            assertTrue(ft.storeTermVectorOffsets());
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .startObject("index_prefixes").endObject()
                .field("term_vector", "with_positions")
                .endObject().endObject().endObject().endObject());

            DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

            FieldMapper prefix = (FieldMapper) mapper.mappers().getMapper("field._index_prefix");
            FieldType ft = prefix.fieldType;
            assertEquals(prefix.name(), "field._index_prefix");
            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, ft.indexOptions());
            assertFalse(ft.storeTermVectorOffsets());
        }
    }

    public void testNestedIndexPrefixes() throws IOException {
        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("properties")
                            .startObject("object")
                                .field("type", "object")
                                .startObject("properties")
                                    .startObject("field")
                                        .field("type", "text")
                                        .startObject("index_prefixes").endObject()
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject());

            indexService.mapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
            MappedFieldType textField = indexService.mapperService().fieldType("object.field");
            assertNotNull(textField);
            assertThat(textField, instanceOf(TextFieldType.class));
            MappedFieldType prefix = ((TextFieldType) textField).getPrefixFieldType();
            assertEquals(prefix.name(), "object.field._index_prefix");
            FieldMapper mapper
                = (FieldMapper) indexService.mapperService().documentMapper().mappers().getMapper("object.field._index_prefix");
            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, mapper.fieldType.indexOptions());
            assertFalse(mapper.fieldType.storeTermVectorOffsets());
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("body")
                            .field("type", "text")
                            .startObject("fields")
                                .startObject("with_prefix")
                                    .field("type", "text")
                                    .startObject("index_prefixes").endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject());

            indexService.mapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
            MappedFieldType textField = indexService.mapperService().fieldType("body.with_prefix");
            assertNotNull(textField);
            assertThat(textField, instanceOf(TextFieldType.class));
            MappedFieldType prefix = ((TextFieldType) textField).getPrefixFieldType();
            assertEquals(prefix.name(), "body.with_prefix._index_prefix");
            FieldMapper mapper
                = (FieldMapper) indexService.mapperService().documentMapper().mappers().getMapper("body.with_prefix._index_prefix");
            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, mapper.fieldType.indexOptions());
            assertFalse(mapper.fieldType.storeTermVectorOffsets());
        }
    }

    public void testFastPhraseMapping() throws IOException {

        QueryShardContext queryShardContext = indexService.newQueryShardContext(
            randomInt(20), null, () -> {
                throw new UnsupportedOperationException();
            }, null);

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("analyzer", "my_stop_analyzer")
            .field("index_phrases", true)
            .endObject()
            .startObject("synfield")
            .field("type", "text")
            .field("analyzer", "standard")  // will be replaced with MockSynonymAnalyzer
            .field("index_phrases", true)
            .endObject()
            .endObject()
            .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        queryShardContext.getMapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        Query q = new MatchPhraseQueryBuilder("field", "two words").toQuery(queryShardContext);
        assertThat(q, is(new PhraseQuery("field._index_phrase", "two words")));

        Query q2 = new MatchPhraseQueryBuilder("field", "three words here").toQuery(queryShardContext);
        assertThat(q2, is(new PhraseQuery("field._index_phrase", "three words", "words here")));

        Query q3 = new MatchPhraseQueryBuilder("field", "two words").slop(1).toQuery(queryShardContext);
        assertThat(q3, is(new PhraseQuery(1, "field", "two", "words")));

        Query q4 = new MatchPhraseQueryBuilder("field", "singleton").toQuery(queryShardContext);
        assertThat(q4, is(new TermQuery(new Term("field", "singleton"))));

        Query q5 = new MatchPhraseQueryBuilder("field", "sparkle a stopword").toQuery(queryShardContext);
        assertThat(q5,
            is(new PhraseQuery.Builder().add(new Term("field", "sparkle")).add(new Term("field", "stopword"), 2).build()));

        MatchQuery matchQuery = new MatchQuery(queryShardContext);
        matchQuery.setAnalyzer(new MockSynonymAnalyzer());
        Query q6 = matchQuery.parse(MatchQuery.Type.PHRASE, "synfield", "motor dogs");
        assertThat(q6, is(new MultiPhraseQuery.Builder()
            .add(new Term[]{
                new Term("synfield._index_phrase", "motor dogs"),
                new Term("synfield._index_phrase", "motor dog")})
            .build()));

        // https://github.com/elastic/elasticsearch/issues/43976
        CannedTokenStream cts = new CannedTokenStream(
            new Token("foo", 1, 0, 2, 2),
            new Token("bar", 0, 0, 2),
            new Token("baz", 1, 0, 2)
        );
        Analyzer synonymAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(reader -> {}, cts);
            }
        };
        matchQuery.setAnalyzer(synonymAnalyzer);
        Query q7 = matchQuery.parse(MatchQuery.Type.BOOLEAN, "synfield", "foo");
        assertThat(q7, is(new BooleanQuery.Builder().add(new BooleanQuery.Builder()
            .add(new TermQuery(new Term("synfield", "foo")), BooleanClause.Occur.SHOULD)
            .add(new PhraseQuery.Builder()
                .add(new Term("synfield._index_phrase", "bar baz"))
                .build(), BooleanClause.Occur.SHOULD)
            .build(), BooleanClause.Occur.SHOULD).build()));

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field", "Some English text that is going to be very useful")
                    .endObject()),
            XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field._index_phrase");
        assertEquals(1, fields.length);

        try (TokenStream ts = fields[0].tokenStream(queryShardContext.getMapperService().indexAnalyzer(), null)) {
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            assertTrue(ts.incrementToken());
            assertEquals("Some English", termAtt.toString());
        }

        {
            String badConfigMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("index", "false")
                .field("index_phrases", true)
                .endObject().endObject()
                .endObject().endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(badConfigMapping))
            );
            assertThat(e.getMessage(), containsString("Cannot set index_phrases on unindexed field [field]"));
        }

        {
            String badConfigMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("index_options", "freqs")
                .field("index_phrases", true)
                .endObject().endObject()
                .endObject().endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(badConfigMapping))
            );
            assertThat(e.getMessage(), containsString("Cannot set index_phrases on field [field] if positions are not enabled"));
        }
    }

    public void testIndexPrefixMapping() throws IOException {

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .startObject("index_prefixes")
                .field("min_chars", 2)
                .field("max_chars", 10)
                .endObject()
                .endObject().endObject()
                .endObject().endObject());

            DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
            assertEquals(mapping, mapper.mappingSource().toString());

            assertThat(mapper.mappers().getMapper("field._index_prefix").toString(), containsString("prefixChars=2:10"));

            ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                    .bytes(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("field", "Some English text that is going to be very useful")
                            .endObject()),
                XContentType.JSON));

            IndexableField[] fields = doc.rootDoc().getFields("field._index_prefix");
            assertEquals(1, fields.length);
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .startObject("index_prefixes").endObject()
                .endObject().endObject()
                .endObject().endObject());
            CompressedXContent json = new CompressedXContent(mapping);
            DocumentMapper mapper = parser.parse("type", json);

            assertThat(mapper.mappers().getMapper("field._index_prefix").toString(), containsString("prefixChars=2:5"));

        }

        {
            String illegalMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .startObject("index_prefixes")
                .field("min_chars", 1)
                .field("max_chars", 10)
                .endObject()
                .startObject("fields")
                .startObject("_index_prefix").field("type", "text").endObject()
                .endObject()
                .endObject().endObject()
                .endObject().endObject());

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
                indexService.mapperService().merge("type", new CompressedXContent(illegalMapping), MergeReason.MAPPING_UPDATE));
            assertThat(e.getMessage(), containsString("Field [field._index_prefix] is defined twice."));

        }

        {
            String badConfigMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .startObject("index_prefixes")
                .field("min_chars", 11)
                .field("max_chars", 10)
                .endObject()
                .endObject().endObject()
                .endObject().endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(badConfigMapping))
            );
            assertThat(e.getMessage(), containsString("min_chars [11] must be less than max_chars [10]"));
        }

        {
            String badConfigMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .startObject("index_prefixes")
                .field("min_chars", 0)
                .field("max_chars", 10)
                .endObject()
                .endObject().endObject()
                .endObject().endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(badConfigMapping))
            );
            assertThat(e.getMessage(), containsString("min_chars [0] must be greater than zero"));
        }

        {
            String badConfigMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .startObject("index_prefixes")
                .field("min_chars", 1)
                .field("max_chars", 25)
                .endObject()
                .endObject().endObject()
                .endObject().endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(badConfigMapping))
            );
            assertThat(e.getMessage(), containsString("max_chars [25] must be less than 20"));
        }

        {
            String badConfigMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("analyzer", "standard")
                .field("index_prefixes", (String) null)
                .endObject().endObject()
                .endObject().endObject());
            MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> parser.parse("type", new CompressedXContent(badConfigMapping))
            );
            assertThat(e.getMessage(), containsString("[index_prefixes] must not have a [null] value"));
        }

        {
            String badConfigMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("index", "false")
                .startObject("index_prefixes").endObject()
                .endObject().endObject()
                .endObject().endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(badConfigMapping))
            );
            assertThat(e.getMessage(), containsString("Cannot set index_prefixes on unindexed field [field]"));
        }
    }

    public void testFastPhrasePrefixes() throws IOException {
        QueryShardContext queryShardContext = indexService.newQueryShardContext(
            randomInt(20), null, () -> {
                throw new UnsupportedOperationException();
            }, null);

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
                .startObject("field")
                    .field("type", "text")
                    .field("analyzer", "my_stop_analyzer")
                    .startObject("index_prefixes")
                        .field("min_chars", 2)
                        .field("max_chars", 10)
                    .endObject()
                .endObject()
                .startObject("synfield")
                    .field("type", "text")
                    .field("analyzer", "standard")  // will be replaced with MockSynonymAnalyzer
                    .field("index_phrases", true)
                    .startObject("index_prefixes")
                        .field("min_chars", 2)
                        .field("max_chars", 10)
                    .endObject()
                .endObject()
            .endObject()
            .endObject().endObject());

        queryShardContext.getMapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "two words").toQuery(queryShardContext);
            Query expected = new SpanNearQuery.Builder("field", true)
                .addClause(new SpanTermQuery(new Term("field", "two")))
                .addClause(new FieldMaskingSpanQuery(
                    new SpanTermQuery(new Term("field._index_prefix", "words")), "field")
                )
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "three words here").toQuery(queryShardContext);
            Query expected = new SpanNearQuery.Builder("field", true)
                .addClause(new SpanTermQuery(new Term("field", "three")))
                .addClause(new SpanTermQuery(new Term("field", "words")))
                .addClause(new FieldMaskingSpanQuery(
                    new SpanTermQuery(new Term("field._index_prefix", "here")), "field")
                )
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "two words").slop(1).toQuery(queryShardContext);
            MultiPhrasePrefixQuery mpq = new MultiPhrasePrefixQuery("field");
            mpq.setSlop(1);
            mpq.add(new Term("field", "two"));
            mpq.add(new Term("field", "words"));
            assertThat(q, equalTo(mpq));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "singleton").toQuery(queryShardContext);
            assertThat(q, is(new SynonymQuery(new Term("field._index_prefix", "singleton"))));
        }

        {

            Query q = new MatchPhrasePrefixQueryBuilder("field", "sparkle a stopword").toQuery(queryShardContext);
            Query expected = new SpanNearQuery.Builder("field", true)
                .addClause(new SpanTermQuery(new Term("field", "sparkle")))
                .addGap(1)
                .addClause(new FieldMaskingSpanQuery(
                    new SpanTermQuery(new Term("field._index_prefix", "stopword")), "field")
                )
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            MatchQuery matchQuery = new MatchQuery(queryShardContext);
            matchQuery.setAnalyzer(new MockSynonymAnalyzer());
            Query q = matchQuery.parse(MatchQuery.Type.PHRASE_PREFIX, "synfield", "motor dogs");
            Query expected = new SpanNearQuery.Builder("synfield", true)
                .addClause(new SpanTermQuery(new Term("synfield", "motor")))
                .addClause(
                    new SpanOrQuery(
                        new FieldMaskingSpanQuery(
                            new SpanTermQuery(new Term("synfield._index_prefix", "dogs")), "synfield"
                        ),
                        new FieldMaskingSpanQuery(
                            new SpanTermQuery(new Term("synfield._index_prefix", "dog")), "synfield"
                        )
                    )
                )
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            MatchQuery matchQuery = new MatchQuery(queryShardContext);
            matchQuery.setPhraseSlop(1);
            matchQuery.setAnalyzer(new MockSynonymAnalyzer());
            Query q = matchQuery.parse(MatchQuery.Type.PHRASE_PREFIX, "synfield", "two dogs");
            MultiPhrasePrefixQuery mpq = new MultiPhrasePrefixQuery("synfield");
            mpq.setSlop(1);
            mpq.add(new Term("synfield", "two"));
            mpq.add(new Term[] { new Term("synfield", "dogs"), new Term("synfield", "dog") });
            assertThat(q, equalTo(mpq));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "motor d").toQuery(queryShardContext);
            MultiPhrasePrefixQuery mpq = new MultiPhrasePrefixQuery("field");
            mpq.add(new Term("field", "motor"));
            mpq.add(new Term("field", "d"));
            assertThat(q, equalTo(mpq));
        }
    }

    public void testSimpleMerge() throws IOException {
        MapperService mapperService = createIndex("test_mapping_merge").mapperService();
        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("a_field")
                            .field("type", "text")
                            .startObject("index_prefixes").endObject()
                            .field("index_phrases", true)
                        .endObject()
                    .endObject()
                .endObject().endObject());
            DocumentMapper mapper = mapperService.merge("_doc",
                new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
            assertThat(mapper.mappers().getMapper("a_field"), instanceOf(TextFieldMapper.class));
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("a_field")
                            .field("type", "text")
                            .startObject("index_prefixes").endObject()
                            .field("index_phrases", true)
                        .endObject()
                    .endObject()
                .endObject().endObject());
            DocumentMapper mapper = mapperService.merge("_doc",
                new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
            assertThat(mapper.mappers().getMapper("a_field"), instanceOf(TextFieldMapper.class));
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("a_field")
                            .field("type", "text")
                                .startObject("index_prefixes")
                                    .field("min_chars", "3")
                                .endObject()
                                .field("index_phrases", true)
                            .endObject()
                    .endObject()
                .endObject().endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapperService.merge("_doc",
                    new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE));
            assertThat(e.getMessage(), containsString("different [index_prefixes]"));
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("a_field")
                            .field("type", "text")
                            .startObject("index_prefixes").endObject()
                            .field("index_phrases", false)
                        .endObject()
                    .endObject()
                .endObject().endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapperService.merge("_doc",
                    new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE));
            assertThat(e.getMessage(), containsString("different [index_phrases]"));
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("a_field")
                            .field("type", "text")
                            .startObject("index_prefixes").endObject()
                            .field("index_phrases", true)
                        .endObject()
                        .startObject("b_field")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject().endObject());
            DocumentMapper mapper = mapperService.merge("_doc",
                new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
            assertThat(mapper.mappers().getMapper("a_field"), instanceOf(TextFieldMapper.class));
            assertThat(mapper.mappers().getMapper("b_field"), instanceOf(KeywordFieldMapper.class));
        }
    }

    public void testMeta() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "text")
                .field("meta", Collections.singletonMap("foo", "bar"))
                .endObject().endObject().endObject().endObject());

        DocumentMapper mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "text")
                .endObject().endObject().endObject().endObject());
        mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping2, mapper.mappingSource().toString());

        String mapping3 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "text")
                .field("meta", Collections.singletonMap("baz", "quux"))
                .endObject().endObject().endObject().endObject());
        mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testParseSourceValue() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        FieldMapper fieldMapper = newBuilder().build(context);
        TextFieldMapper mapper = (TextFieldMapper) fieldMapper;

        assertEquals("value", mapper.parseSourceValue("value", null));
        assertEquals("42", mapper.parseSourceValue(42L, null));
        assertEquals("true", mapper.parseSourceValue(true, null));
    }
}
