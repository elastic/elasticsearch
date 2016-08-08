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

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.core.TextFieldMapper.TextFieldType;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TextFieldMapperTests extends ESSingleNodeTestCase {

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void before() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    public void testDefaults() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "text").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

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
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "text").field("store", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertTrue(fields[0].fieldType().stored());
    }

    public void testDisableIndex() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "text").field("index", false).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testDisableNorms() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
                    .field("norms", false)
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

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
        String mapping = mappingBuilder.endObject().endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        XContentBuilder jsonDoc = XContentFactory.jsonBuilder().startObject();
        for (String option : supportedOptions.keySet()) {
            jsonDoc.field(option, "1234");
        }
        ParsedDocument doc = mapper.parse("test", "type", "1", jsonDoc.endObject().bytes());

        for (Map.Entry<String, IndexOptions> entry : supportedOptions.entrySet()) {
            String field = entry.getKey();
            IndexOptions options = entry.getValue();
            IndexableField[] fields = doc.rootDoc().getFields(field);
            assertEquals(1, fields.length);
            assertEquals(options, fields[0].fieldType().indexOptions());
        }
    }

    public void testDefaultPositionIncrementGap() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "text").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = indexService.mapperService().merge("type",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE, false);

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", new String[] {"a", "b"})
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals("a", fields[0].stringValue());
        assertEquals("b", fields[1].stringValue());

        IndexShard shard = indexService.getShard(0);
        shard.index(new Engine.Index(new Term("_uid", "1"), doc));
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
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
                    .field("position_increment_gap", positionIncrementGap)
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = indexService.mapperService().merge("type",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE, false);

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", new String[] {"a", "b"})
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals("a", fields[0].stringValue());
        assertEquals("b", fields[1].stringValue());

        IndexShard shard = indexService.getShard(0);
        shard.index(new Engine.Index(new Term("_uid", "1"), doc));
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
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "text")
                        .field("analyzer", "standard")
                        .field("search_analyzer", "keyword")
                    .endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        // special case: default index analyzer
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "text")
                        .field("analyzer", "default")
                        .field("search_analyzer", "keyword")
                    .endObject()
                .endObject().endObject().endObject().string();

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("analyzer", "keyword")
            .endObject()
            .endObject().endObject().endObject().string();

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        // special case: default search analyzer
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("analyzer", "keyword")
            .field("search_analyzer", "default")
            .endObject()
            .endObject().endObject().endObject().string();

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("analyzer", "keyword")
            .endObject()
            .endObject().endObject().endObject().string();
        mapper = parser.parse("type", new CompressedXContent(mapping));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        mapper.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true")));
        builder.endObject();

        String mappingString = builder.string();
        assertTrue(mappingString.contains("analyzer"));
        assertTrue(mappingString.contains("search_analyzer"));
        assertTrue(mappingString.contains("search_quote_analyzer"));
    }

    public void testSearchQuoteAnalyzerSerialization() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "text")
                        .field("analyzer", "standard")
                        .field("search_analyzer", "standard")
                        .field("search_quote_analyzer", "keyword")
                    .endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        // special case: default index/search analyzer
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "text")
                        .field("analyzer", "default")
                        .field("search_analyzer", "default")
                        .field("search_quote_analyzer", "keyword")
                    .endObject()
                .endObject().endObject().endObject().string();

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());
    }

    public void testTermVectors() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
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
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
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

    public void testEagerGlobalOrdinals() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
                    .field("eager_global_ordinals", true)
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());
        assertTrue(mapper.mappers().getMapper("field").fieldType().eagerGlobalOrdinals());
    }

    public void testFielddata() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper disabledMapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, disabledMapper.mappingSource().toString());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> disabledMapper.mappers().getMapper("field").fieldType().fielddataBuilder());
        assertThat(e.getMessage(), containsString("Fielddata is disabled"));

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
                    .field("fielddata", true)
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper enabledMapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, enabledMapper.mappingSource().toString());
        enabledMapper.mappers().getMapper("field").fieldType().fielddataBuilder(); // no exception this time

        String illegalMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
                    .field("index", false)
                    .field("fielddata", true)
                .endObject().endObject()
                .endObject().endObject().string();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(illegalMapping)));
        assertThat(ex.getMessage(), containsString("Cannot enable fielddata on a [text] field that is not indexed"));
    }

    public void testFrequencyFilter() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "text")
                    .field("fielddata", true)
                    .startObject("fielddata_frequency_filter")
                        .field("min", 2d)
                        .field("min_segment_size", 1000)
                    .endObject()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());
        TextFieldType fieldType = (TextFieldType) mapper.mappers().getMapper("field").fieldType();
        assertThat(fieldType.fielddataMinFrequency(), equalTo(2d));
        assertThat(fieldType.fielddataMaxFrequency(), equalTo((double) Integer.MAX_VALUE));
        assertThat(fieldType.fielddataMinSegmentSize(), equalTo(1000));
    }

    public void testNullConfigValuesFail() throws MapperParsingException, IOException {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "text")
                            .field("analyzer", (String) null)
                        .endObject()
                    .endObject()
                .endObject().endObject().string();

        Exception e = expectThrows(MapperParsingException.class, () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("[analyzer] must not have a [null] value", e.getMessage());
    }

    public void testNotIndexedFieldPositionIncrement() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
            .field("type", "text")
            .field("index", false)
            .field("position_increment_gap", 10)
            .endObject().endObject().endObject().endObject().string();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("Cannot set position_increment_gap on field [field] without positions enabled", e.getMessage());
    }

    public void testAnalyzedFieldPositionIncrementWithoutPositions() throws IOException {
        for (String indexOptions : Arrays.asList("docs", "freqs")) {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "text")
                .field("index_options", indexOptions)
                .field("position_increment_gap", 10)
                .endObject().endObject().endObject().endObject().string();

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping)));
            assertEquals("Cannot set position_increment_gap on field [field] without positions enabled", e.getMessage());
        }
    }
}
