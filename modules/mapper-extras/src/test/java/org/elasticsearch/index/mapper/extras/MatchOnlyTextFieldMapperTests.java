/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.hamcrest.Matchers;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class MatchOnlyTextFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "value";
    }

    public void testExistsStandardSource() throws IOException {
        assertExistsQuery(createMapperService(fieldMapping(b -> b.field("type", "match_only_text"))));
    }

    public void testExistsSyntheticSource() throws IOException {
        assertExistsQuery(createSytheticSourceMapperService(fieldMapping(b -> b.field("type", "match_only_text"))));
    }

    public void testPhraseQueryStandardSource() throws IOException {
        assertPhraseQuery(createMapperService(fieldMapping(b -> b.field("type", "match_only_text"))));
    }

    public void testPhraseQuerySyntheticSource() throws IOException {
        assertPhraseQuery(createSytheticSourceMapperService(fieldMapping(b -> b.field("type", "match_only_text"))));
    }

    private void assertPhraseQuery(MapperService mapperService) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            LuceneDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "the quick brown fox"))).rootDoc();
            iw.addDocument(doc);
            iw.close();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SearchExecutionContext context = createSearchExecutionContext(mapperService, newSearcher(reader));
                MatchPhraseQueryBuilder queryBuilder = new MatchPhraseQueryBuilder("field", "brown fox");
                TopDocs docs = context.searcher().search(queryBuilder.toQuery(context), 1);
                assertThat(docs.totalHits.value(), equalTo(1L));
                assertThat(docs.totalHits.relation(), equalTo(TotalHits.Relation.EQUAL_TO));
                assertThat(docs.scoreDocs[0].doc, equalTo(0));
            }
        }
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(
            b -> { b.field("meta", Collections.singletonMap("format", "mysql.access")); },
            m -> assertEquals(Collections.singletonMap("format", "mysql.access"), m.fieldType().meta())
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "match_only_text");
    }

    @Override
    protected void minimalStoreMapping(XContentBuilder b) throws IOException {
        // 'store' is always true
        minimalMapping(b);
    }

    public void testDefaults() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals("1234", fields.get(0).stringValue());
        IndexableFieldType fieldType = fields.get(0).fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertTrue(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());
    }

    public void testNullConfigValuesFail() throws MapperParsingException {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "match_only_text").field("meta", (String) null)))
        );
        assertThat(e.getMessage(), containsString("[meta] on mapper [field] of type [match_only_text] must not have a [null] value"));
    }

    public void testSimpleMerge() throws IOException {
        XContentBuilder startingMapping = fieldMapping(b -> b.field("type", "match_only_text"));
        MapperService mapperService = createMapperService(startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(MatchOnlyTextFieldMapper.class));

        merge(mapperService, startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(MatchOnlyTextFieldMapper.class));

        XContentBuilder newField = mapping(b -> {
            b.startObject("field").field("type", "match_only_text").startObject("meta").field("key", "value").endObject().endObject();
            b.startObject("other_field").field("type", "keyword").endObject();
        });
        merge(mapperService, newField);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(MatchOnlyTextFieldMapper.class));
        assertThat(mapperService.documentMapper().mappers().getMapper("other_field"), instanceOf(KeywordFieldMapper.class));
    }

    public void testDisabledSource() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        {
            mapping.startObject("properties");
            {
                mapping.startObject("foo");
                {
                    mapping.field("type", "match_only_text");
                }
                mapping.endObject();
            }
            mapping.endObject();

            mapping.startObject("_source");
            {
                mapping.field("enabled", false);
            }
            mapping.endObject();
        }
        mapping.endObject().endObject();

        MapperService mapperService = createMapperService(mapping);
        MappedFieldType ft = mapperService.fieldType("foo");
        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 4, 7));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ft.phraseQuery(ts, 0, true, context));
        assertThat(e.getMessage(), Matchers.containsString("cannot run positional queries since [_source] is disabled"));

        // Term queries are ok
        ft.termQuery("a", context); // no exception
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("We don't have a way to assert things here", true);
        return null;
    }

    @Override
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        assumeFalse("We don't have a way to assert things here", true);
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        assertFalse("match_only_text doesn't support ignoreMalformed", ignoreMalformed);
        return new MatchOnlyTextSyntheticSourceSupport();
    }

    static class MatchOnlyTextSyntheticSourceSupport implements SyntheticSourceSupport {
        @Override
        public SyntheticSourceExample example(int maxValues) {
            if (randomBoolean()) {
                Tuple<String, String> v = generateValue();
                return new SyntheticSourceExample(v.v1(), v.v2(), this::mapping);
            }
            List<Tuple<String, String>> values = randomList(1, maxValues, this::generateValue);
            List<String> in = values.stream().map(Tuple::v1).toList();
            List<String> outList = values.stream().map(Tuple::v2).toList();
            Object out = outList.size() == 1 ? outList.get(0) : outList;
            return new SyntheticSourceExample(in, out, this::mapping);
        }

        private Tuple<String, String> generateValue() {
            String v = randomList(1, 10, () -> randomAlphaOfLength(5)).stream().collect(Collectors.joining(" "));
            return Tuple.tuple(v, v);
        }

        private void mapping(XContentBuilder b) throws IOException {
            b.field("type", "match_only_text");
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of();
        }
    }

    public void testDocValues() throws IOException {
        MapperService mapper = createMapperService(fieldMapping(b -> b.field("type", "match_only_text")));
        assertScriptDocValues(mapper, "foo", equalTo(List.of("foo")));
    }

    public void testDocValuesLoadedFromSynthetic() throws IOException {
        MapperService mapper = createSytheticSourceMapperService(fieldMapping(b -> b.field("type", "match_only_text")));
        assertScriptDocValues(mapper, "foo", equalTo(List.of("foo")));
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
