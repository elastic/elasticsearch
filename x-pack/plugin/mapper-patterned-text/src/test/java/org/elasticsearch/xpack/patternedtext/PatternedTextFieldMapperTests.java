/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.DateFieldMapper;
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
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class PatternedTextFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new PatternedTextMapperPlugin());
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "value";
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        assertThat(query, instanceOf(FieldExistsQuery.class));
        FieldExistsQuery fieldExistsQuery = (FieldExistsQuery) query;
        assertThat(fieldExistsQuery.getField(), startsWith("field"));
        assertNoFieldNamesField(fields);
    }

    public void testExistsStandardSource() throws IOException {
        assertExistsQuery(createMapperService(fieldMapping(b -> b.field("type", "patterned_text"))));
    }

    public void testExistsSyntheticSource() throws IOException {
        assertExistsQuery(createSytheticSourceMapperService(fieldMapping(b -> b.field("type", "patterned_text"))));
    }

    public void testPhraseQueryStandardSource() throws IOException {
        assertPhraseQuery(createMapperService(fieldMapping(b -> b.field("type", "patterned_text"))));
    }

    public void testPhraseQuerySyntheticSource() throws IOException {
        assertPhraseQuery(createSytheticSourceMapperService(fieldMapping(b -> b.field("type", "patterned_text"))));
    }

    private void assertPhraseQuery(MapperService mapperService) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            LuceneDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "the quick brown fox 1"))).rootDoc();
            iw.addDocument(doc);
            iw.close();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SearchExecutionContext context = createSearchExecutionContext(mapperService, newSearcher(reader));
                MatchPhraseQueryBuilder queryBuilder = new MatchPhraseQueryBuilder("field", "brown fox 1");
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
        b.field("type", "patterned_text");
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
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "patterned_text").field("meta", (String) null)))
        );
        assertThat(e.getMessage(), containsString("[meta] on mapper [field] of type [patterned_text] must not have a [null] value"));
    }

    public void testSimpleMerge() throws IOException {
        XContentBuilder startingMapping = fieldMapping(b -> b.field("type", "patterned_text"));
        MapperService mapperService = createMapperService(startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(PatternedTextFieldMapper.class));

        merge(mapperService, startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(PatternedTextFieldMapper.class));

        XContentBuilder newField = mapping(b -> {
            b.startObject("field").field("type", "patterned_text").startObject("meta").field("key", "value").endObject().endObject();
            b.startObject("other_field").field("type", "keyword").endObject();
        });
        merge(mapperService, newField);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(PatternedTextFieldMapper.class));
        assertThat(mapperService.documentMapper().mappers().getMapper("other_field"), instanceOf(KeywordFieldMapper.class));
    }

    public void testDisabledSource() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        {
            mapping.startObject("properties");
            {
                mapping.startObject("foo");
                {
                    mapping.field("type", "patterned_text");
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

        // Allowed even if source is disabled.
        ft.phraseQuery(ts, 0, true, context);
        ft.termQuery("a", context);
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
        assertFalse("patterned_text doesn't support ignoreMalformed", ignoreMalformed);
        return new PatternedTextSyntheticSourceSupport();
    }

    static class PatternedTextSyntheticSourceSupport implements SyntheticSourceSupport {
        @Override
        public SyntheticSourceExample example(int maxValues) {
            Tuple<String, String> v = generateValue();
            return new SyntheticSourceExample(v.v1(), v.v2(), this::mapping);
        }

        private Tuple<String, String> generateValue() {
            StringBuilder builder = new StringBuilder();
            if (randomBoolean()) {
                builder.append(randomAlphaOfLength(5));
            } else {
                String timestamp = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
                builder.append(timestamp);
            }
            for (int i = 0; i < randomIntBetween(0, 9); i++) {
                builder.append(" ");
                int rand = randomIntBetween(0, 4);
                switch (rand) {
                    case 0 -> builder.append(randomAlphaOfLength(5));
                    case 1 -> builder.append(randomAlphanumericOfLength(5));
                    case 2 -> builder.append(UUID.randomUUID());
                    case 3 -> builder.append(randomIp(true));
                    case 4 -> builder.append(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(randomMillisUpToYear9999()));
                }
            }
            String value = builder.toString();
            return Tuple.tuple(value, value);
        }

        private void mapping(XContentBuilder b) throws IOException {
            b.field("type", "patterned_text");
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of();
        }
    }

    public void testDocValues() throws IOException {
        MapperService mapper = createMapperService(fieldMapping(b -> b.field("type", "patterned_text")));
        assertScriptDocValues(mapper, "foo", equalTo(List.of("foo")));
    }

    public void testDocValuesSynthetic() throws IOException {
        MapperService mapper = createSytheticSourceMapperService(fieldMapping(b -> b.field("type", "patterned_text")));
        assertScriptDocValues(mapper, "foo", equalTo(List.of("foo")));
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
