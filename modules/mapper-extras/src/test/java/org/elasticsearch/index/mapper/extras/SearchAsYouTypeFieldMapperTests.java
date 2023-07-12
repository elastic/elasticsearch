/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.FieldMaskingSpanQuery;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.extras.SearchAsYouTypeFieldMapper.PrefixFieldMapper;
import org.elasticsearch.index.mapper.extras.SearchAsYouTypeFieldMapper.PrefixFieldType;
import org.elasticsearch.index.mapper.extras.SearchAsYouTypeFieldMapper.SearchAsYouTypeAnalyzer;
import org.elasticsearch.index.mapper.extras.SearchAsYouTypeFieldMapper.SearchAsYouTypeFieldType;
import org.elasticsearch.index.mapper.extras.SearchAsYouTypeFieldMapper.ShingleFieldMapper;
import org.elasticsearch.index.mapper.extras.SearchAsYouTypeFieldMapper.ShingleFieldType;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.QueryStringQueryParser;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class SearchAsYouTypeFieldMapperTests extends MapperTestCase {

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("max_shingle_size", b -> b.field("max_shingle_size", 4));
        checker.registerConflictCheck("similarity", b -> b.field("similarity", "boolean"));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("analyzer", b -> b.field("analyzer", "keyword"));
        checker.registerConflictCheck("index_options", b -> b.field("index_options", "docs"));
        checker.registerConflictCheck("term_vector", b -> b.field("term_vector", "yes"));

        // norms can be set from true to false, but not vice versa
        checker.registerConflictCheck("norms", fieldMapping(b -> {
            b.field("type", "text");
            b.field("norms", false);
        }), fieldMapping(b -> {
            b.field("type", "text");
            b.field("norms", true);
        }));
        checker.registerUpdateCheck(b -> {
            b.field("type", "search_as_you_type");
            b.field("norms", true);
        }, b -> {
            b.field("type", "search_as_you_type");
            b.field("norms", false);
        }, m -> assertFalse(m.fieldType().getTextSearchInfo().hasNorms()));

        checker.registerUpdateCheck(b -> {
            b.field("analyzer", "default");
            b.field("search_analyzer", "keyword");
        }, m -> assertEquals("keyword", m.fieldType().getTextSearchInfo().searchAnalyzer().name()));
        checker.registerUpdateCheck(b -> {
            b.field("analyzer", "default");
            b.field("search_analyzer", "keyword");
            b.field("search_quote_analyzer", "keyword");
        }, m -> assertEquals("keyword", m.fieldType().getTextSearchInfo().searchQuoteAnalyzer().name()));

    }

    @Override
    protected Object getSampleValueForDocument() {
        return "new york city";
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        NamedAnalyzer dflt = new NamedAnalyzer(
            "default",
            AnalyzerScope.INDEX,
            new StandardAnalyzer(),
            TextFieldMapper.Defaults.POSITION_INCREMENT_GAP
        );
        NamedAnalyzer standard = new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer());
        NamedAnalyzer keyword = new NamedAnalyzer("keyword", AnalyzerScope.INDEX, new KeywordAnalyzer());
        NamedAnalyzer simple = new NamedAnalyzer("simple", AnalyzerScope.INDEX, new SimpleAnalyzer());
        NamedAnalyzer whitespace = new NamedAnalyzer("whitespace", AnalyzerScope.INDEX, new WhitespaceAnalyzer());
        return IndexAnalyzers.of(
            Map.of("default", dflt, "standard", standard, "keyword", keyword, "simple", simple, "whitespace", whitespace)
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "search_as_you_type");
    }

    @Override
    protected void metaMapping(XContentBuilder b) throws IOException {
        // We serialize these fields regardless of whether or not they are changed
        b.field("type", "search_as_you_type").field("max_shingle_size", 3).field("doc_values", false);
    }

    public void testIndexing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "new york city")));
        for (String field : new String[] { "field", "field._index_prefix", "field._2gram", "field._3gram" }) {
            List<IndexableField> fields = doc.rootDoc().getFields(field);
            assertEquals(1, fields.size());
            assertEquals("new york city", fields.get(0).stringValue());
        }
    }

    public void testDefaultConfiguration() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SearchAsYouTypeFieldMapper rootMapper = getRootFieldMapper(defaultMapper, "field");
        assertSearchAsYouTypeFieldMapper(rootMapper, 3, "default");

        PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(defaultMapper, "field._index_prefix");
        assertPrefixFieldType(prefixFieldMapper, rootMapper.indexAnalyzers(), 3, "default");

        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "field._2gram"),
            rootMapper.indexAnalyzers(),
            2,
            "default",
            prefixFieldMapper.fieldType()
        );
        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "field._3gram"),
            rootMapper.indexAnalyzers(),
            3,
            "default",
            prefixFieldMapper.fieldType()
        );
    }

    public void testConfiguration() throws IOException {
        int maxShingleSize = 4;
        String analyzerName = "simple";
        DocumentMapper defaultMapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "search_as_you_type").field("analyzer", analyzerName).field("max_shingle_size", maxShingleSize)
            )
        );

        SearchAsYouTypeFieldMapper rootMapper = getRootFieldMapper(defaultMapper, "field");
        assertSearchAsYouTypeFieldMapper(rootMapper, maxShingleSize, analyzerName);

        PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(defaultMapper, "field._index_prefix");
        assertPrefixFieldType(prefixFieldMapper, rootMapper.indexAnalyzers(), maxShingleSize, analyzerName);

        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "field._2gram"),
            rootMapper.indexAnalyzers(),
            2,
            analyzerName,
            prefixFieldMapper.fieldType()
        );
        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "field._3gram"),
            rootMapper.indexAnalyzers(),
            3,
            analyzerName,
            prefixFieldMapper.fieldType()
        );
        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "field._4gram"),
            rootMapper.indexAnalyzers(),
            4,
            analyzerName,
            prefixFieldMapper.fieldType()
        );
    }

    public void testSimpleMerge() throws IOException {
        MapperService mapperService = createMapperService(
            mapping(b -> b.startObject("a_field").field("type", "search_as_you_type").field("analyzer", "standard").endObject())
        );
        merge(mapperService, mapping(b -> {
            b.startObject("a_field").field("type", "search_as_you_type").field("analyzer", "standard").endObject();
            b.startObject("b_field").field("type", "text").endObject();
        }));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("a_field");
            {
                b.field("type", "search_as_you_type");
                b.field("analyzer", "standard");
                b.field("max_shingle_size", "4");
            }
            b.endObject();
            b.startObject("b_field").field("type", "text").endObject();
        })));
        assertThat(e.getMessage(), containsString("Cannot update parameter [max_shingle_size]"));
    }

    public void testMultiFields() throws IOException {
        for (int shingleSize = 2; shingleSize < 4; shingleSize++) {
            int size = shingleSize;
            MapperService mapperService = createMapperService(fieldMapping(b -> {
                b.field("type", "text");
                b.startObject("fields");
                {
                    b.startObject("suggest").field("type", "search_as_you_type").field("max_shingle_size", size).endObject();
                }
                b.endObject();
            }));
            assertMultiField(shingleSize, mapperService, "field.suggest", "field");
        }
        for (int shingleSize = 2; shingleSize < 4; shingleSize++) {
            String path = "field";
            int size = shingleSize;
            MapperService mapperService = createMapperService(fieldMapping(b -> {
                b.field("type", "search_as_you_type").field("max_shingle_size", size);
                b.startObject("fields");
                {
                    b.startObject("text").field("type", "text").endObject();
                }
                b.endObject();
            }));
            assertMultiField(shingleSize, mapperService, "field", "field.text");

            Mapper mapper = mapperService.mappingLookup().getMapper("field");
            assertThat(mapper, instanceOf(SearchAsYouTypeFieldMapper.class));
            assertSearchAsYouTypeFieldMapper((SearchAsYouTypeFieldMapper) mapper, size, "default");
        }
    }

    private void assertMultiField(int shingleSize, MapperService mapperService, String suggestPath, String textPath) throws IOException {
        List<String> fields = new ArrayList<>();
        fields.add(suggestPath);
        fields.add(textPath);
        MappedFieldType fieldType = mapperService.fieldType(suggestPath + "._index_prefix");
        assertThat(fieldType, instanceOf(PrefixFieldType.class));
        PrefixFieldType prefixFieldType = (PrefixFieldType) fieldType;
        assertEquals(suggestPath, prefixFieldType.parentField);
        for (int i = 2; i < shingleSize; i++) {
            String name = suggestPath + "._" + i + "gram";
            fields.add(name);
            fieldType = mapperService.fieldType(name);
            assertThat(fieldType, instanceOf(ShingleFieldType.class));
            ShingleFieldType ft = (ShingleFieldType) fieldType;
            assertEquals(i, ft.shingleSize);
            assertSame(prefixFieldType, ft.prefixFieldType);
        }

        MappedFieldType textFieldType = mapperService.fieldType(textPath);
        assertThat(textFieldType, instanceOf(TextFieldMapper.TextFieldType.class));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "new york city")));
        for (String field : fields) {
            List<IndexableField> indexFields = doc.rootDoc().getFields(field);
            assertEquals(1, indexFields.size());
            assertEquals("new york city", indexFields.get(0).stringValue());
        }
    }

    private static IndexableFieldType fieldType(ParsedDocument doc, String field) {
        return doc.rootDoc().getField(field).fieldType();
    }

    public void testIndexOptions() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "search_as_you_type").field("index_options", "offsets"))
        );

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "some text")));

        assertThat(fieldType(doc, "field").indexOptions(), equalTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS));

        Stream.of(fieldType(doc, "field._index_prefix"), fieldType(doc, "field._2gram"), fieldType(doc, "field._3gram"))
            .forEach(ft -> assertThat(ft.indexOptions(), equalTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)));
    }

    public void testStore() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "search_as_you_type").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "some text")));

        assertTrue(fieldType(doc, "field").stored());
        Stream.of(fieldType(doc, "field._index_prefix"), fieldType(doc, "field._2gram"), fieldType(doc, "field._3gram"))
            .forEach(ft -> assertFalse(ft.stored()));
    }

    public void testIndex() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "search_as_you_type").field("index", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "some text")));
        assertNull(doc.rootDoc().getField("field"));
    }

    public void testStoredOnly() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "search_as_you_type");
            b.field("index", false);
            b.field("store", true);
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "some text")));
        assertTrue(fieldType(doc, "field").stored());
        assertThat(fieldType(doc, "field").indexOptions(), equalTo(IndexOptions.NONE));
        assertNull(doc.rootDoc().getField("field._index_prefix"));
        assertNull(doc.rootDoc().getField("field._2gram"));
        assertNull(doc.rootDoc().getField("field._3gram"));
    }

    public void testTermVectors() throws IOException {
        for (String termVector : new String[] {
            "yes",
            "with_positions",
            "with_offsets",
            "with_positions_offsets",
            "with_positions_payloads",
            "with_positions_offsets_payloads" }) {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", "search_as_you_type").field("term_vector", termVector))
            );
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", "some text")));

            IndexableFieldType rootField = fieldType(doc, "field");
            assertTrue(rootField.storeTermVectors());
            if (termVector.contains("positions")) {
                assertThat(rootField.storeTermVectorPositions(), equalTo(termVector.contains("positions")));
            }
            if (termVector.contains("offsets")) {
                assertTrue(rootField.storeTermVectorOffsets());
                assertThat(rootField.storeTermVectorOffsets(), equalTo(termVector.contains("offsets")));
            }
            if (termVector.contains("payloads")) {
                assertTrue(rootField.storeTermVectorPayloads());
                assertThat(rootField.storeTermVectorPayloads(), equalTo(termVector.contains("payloads")));
            }

            Stream.of(fieldType(doc, "field._2gram"), fieldType(doc, "field._3gram")).forEach(ft -> {
                assertTrue(ft.storeTermVectors());
                if (termVector.contains("positions")) {
                    assertThat(ft.storeTermVectorPositions(), equalTo(termVector.contains("positions")));
                }
                if (termVector.contains("offsets")) {
                    assertThat(ft.storeTermVectorOffsets(), equalTo(termVector.contains("offsets")));
                }
                if (termVector.contains("payloads")) {
                    assertThat(ft.storeTermVectorPayloads(), equalTo(termVector.contains("payloads")));
                }
            });

            PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(mapper, "field._index_prefix");
            assertFalse(prefixFieldMapper.fieldType.storeTermVectors());
            assertFalse(prefixFieldMapper.fieldType.storeTermVectorOffsets());
            assertFalse(prefixFieldMapper.fieldType.storeTermVectorPositions());
            assertFalse(prefixFieldMapper.fieldType.storeTermVectorPayloads());
        }
    }

    public void testNorms() throws IOException {
        // default setting
        {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", "some text")));

            Stream.of(fieldType(doc, "field"), fieldType(doc, "field._2gram"), fieldType(doc, "field._3gram"))
                .forEach(ft -> assertFalse(ft.omitNorms()));

            PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(mapper, "field._index_prefix");
            assertTrue(prefixFieldMapper.fieldType.omitNorms());
        }

        // can disable norms on search_as_you_type fields
        {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "search_as_you_type").field("norms", false)));
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", "some text")));

            assertTrue(fieldType(doc, "field").omitNorms());

            Stream.of(fieldType(doc, "field._index_prefix"), fieldType(doc, "field._2gram"), fieldType(doc, "field._3gram"))
                .forEach(ft -> assertTrue(ft.omitNorms()));
        }
    }

    public void testDocumentParsingSingleValue() throws IOException {
        documentParsingTestCase(Collections.singleton(randomAlphaOfLengthBetween(5, 20)));
    }

    public void testDocumentParsingMultipleValues() throws IOException {
        documentParsingTestCase(randomUnique(() -> randomAlphaOfLengthBetween(3, 20), randomIntBetween(2, 10)));
    }

    public void testMatchPhrasePrefix() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(fieldMapping(this::minimalMapping))
        );
        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "two words").toQuery(searchExecutionContext);
            Query expected = new SynonymQuery.Builder("field._index_prefix").addTerm(new Term("field._index_prefix", "two words")).build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "three words here").toQuery(searchExecutionContext);
            Query expected = new SynonymQuery.Builder("field._index_prefix").addTerm(new Term("field._index_prefix", "three words here"))
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "two words").slop(1).toQuery(searchExecutionContext);
            MultiPhrasePrefixQuery mpq = new MultiPhrasePrefixQuery("field");
            mpq.setSlop(1);
            mpq.add(new Term("field", "two"));
            mpq.add(new Term("field", "words"));
            assertThat(q, equalTo(mpq));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "more than three words").toQuery(searchExecutionContext);
            Query expected = new SpanNearQuery.Builder("field._3gram", true).addClause(
                new SpanTermQuery(new Term("field._3gram", "more than three"))
            )
                .addClause(
                    new FieldMaskingSpanQuery(new SpanTermQuery(new Term("field._index_prefix", "than three words")), "field._3gram")
                )
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field._3gram", "more than three words").toQuery(searchExecutionContext);
            Query expected = new SpanNearQuery.Builder("field._3gram", true).addClause(
                new SpanTermQuery(new Term("field._3gram", "more than three"))
            )
                .addClause(
                    new FieldMaskingSpanQuery(new SpanTermQuery(new Term("field._index_prefix", "than three words")), "field._3gram")
                )
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field._3gram", "two words").toQuery(searchExecutionContext);
            Query expected = new MatchNoDocsQuery();
            assertThat(q, equalTo(expected));
        }

        {
            Query actual = new MatchPhrasePrefixQueryBuilder("field._3gram", "one two three four").slop(1).toQuery(searchExecutionContext);
            MultiPhrasePrefixQuery expected = new MultiPhrasePrefixQuery("field._3gram");
            expected.setSlop(1);
            expected.add(new Term("field._3gram", "one two three"));
            expected.add(new Term("field._3gram", "two three four"));
            assertThat(actual, equalTo(expected));
        }

    }

    public void testMatchPhrase() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(fieldMapping(this::minimalMapping))
        );
        {
            Query actual = new MatchPhraseQueryBuilder("field", "one").toQuery(searchExecutionContext);
            Query expected = new TermQuery(new Term("field", "one"));
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field", "one two").toQuery(searchExecutionContext);
            Query expected = new MultiPhraseQuery.Builder().add(new Term("field._2gram", "one two")).build();
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field", "one two three").toQuery(searchExecutionContext);
            Query expected = new MultiPhraseQuery.Builder().add(new Term("field._3gram", "one two three")).build();
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field", "one two three four").toQuery(searchExecutionContext);
            Query expected = new MultiPhraseQuery.Builder().add(new Term("field._3gram", "one two three"))
                .add(new Term("field._3gram", "two three four"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field", "one two").slop(1).toQuery(searchExecutionContext);
            Query expected = new MultiPhraseQuery.Builder().add(new Term("field", "one")).add(new Term("field", "two")).setSlop(1).build();
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field._2gram", "one two").toQuery(searchExecutionContext);
            Query expected = new TermQuery(new Term("field._2gram", "one two"));
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field._2gram", "one two three").toQuery(searchExecutionContext);
            Query expected = new MultiPhraseQuery.Builder().add(new Term("field._2gram", "one two"))
                .add(new Term("field._2gram", "two three"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field._3gram", "one two three").toQuery(searchExecutionContext);
            Query expected = new TermQuery(new Term("field._3gram", "one two three"));
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field._3gram", "one two three four").toQuery(searchExecutionContext);
            Query expected = new MultiPhraseQuery.Builder().add(new Term("field._3gram", "one two three"))
                .add(new Term("field._3gram", "two three four"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            expectThrows(
                IllegalArgumentException.class,
                () -> new MatchPhraseQueryBuilder("field._index_prefix", "one two three four").toQuery(searchExecutionContext)
            );
        }
    }

    public void testNestedExistsQuery() throws IOException, ParseException {
        MapperService ms = createMapperService(mapping(b -> {
            b.startObject("foo");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("bar");
                    {
                        b.field("type", "search_as_you_type");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        SearchExecutionContext context = createSearchExecutionContext(ms);
        QueryStringQueryParser parser = new QueryStringQueryParser(context, "f");
        Query q = parser.parse("foo:*");
        assertEquals(
            new ConstantScoreQuery(
                new BooleanQuery.Builder().add(new FieldExistsQuery("foo.bar"), BooleanClause.Occur.SHOULD)
                    .add(new FieldExistsQuery("foo.bar._3gram"), BooleanClause.Occur.SHOULD)
                    .add(new FieldExistsQuery("foo.bar._2gram"), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term("_field_names", "foo.bar._index_prefix")), BooleanClause.Occur.SHOULD)
                    .build()
            ),
            q
        );
    }

    private static BooleanQuery buildBoolPrefixQuery(String shingleFieldName, String prefixFieldName, List<String> terms) {
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (int i = 0; i < terms.size() - 1; i++) {
            final String term = terms.get(i);
            builder.add(new BooleanClause(new TermQuery(new Term(shingleFieldName, term)), BooleanClause.Occur.SHOULD));
        }
        final String finalTerm = terms.get(terms.size() - 1);
        builder.add(
            new BooleanClause(new ConstantScoreQuery(new TermQuery(new Term(prefixFieldName, finalTerm))), BooleanClause.Occur.SHOULD)
        );
        return builder.build();
    }

    public void testMultiMatchBoolPrefix() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(fieldMapping(b -> b.field("type", "search_as_you_type").field("max_shingle_size", 4)))
        );

        MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder(
            "quick brown fox jump lazy dog",
            "field",
            "field._2gram",
            "field._3gram",
            "field._4gram"
        );
        builder.type(MultiMatchQueryBuilder.Type.BOOL_PREFIX);

        final Query actual = builder.toQuery(searchExecutionContext);
        assertThat(actual, instanceOf(DisjunctionMaxQuery.class));
        final DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) actual;
        assertThat(disMaxQuery.getDisjuncts(), hasSize(4));
        assertThat(
            disMaxQuery.getDisjuncts(),
            containsInAnyOrder(
                buildBoolPrefixQuery("field", "field._index_prefix", asList("quick", "brown", "fox", "jump", "lazy", "dog")),
                buildBoolPrefixQuery(
                    "field._2gram",
                    "field._index_prefix",
                    asList("quick brown", "brown fox", "fox jump", "jump lazy", "lazy dog")
                ),
                buildBoolPrefixQuery(
                    "field._3gram",
                    "field._index_prefix",
                    asList("quick brown fox", "brown fox jump", "fox jump lazy", "jump lazy dog")
                ),
                buildBoolPrefixQuery(
                    "field._4gram",
                    "field._index_prefix",
                    asList("quick brown fox jump", "brown fox jump lazy", "fox jump lazy dog")
                )
            )
        );
    }

    private void documentParsingTestCase(Collection<String> values) throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        final ParsedDocument parsedDocument = defaultMapper.parse(source(b -> {
            if (values.size() > 1) {
                b.array("field", values.toArray(new String[0]));
            } else {
                b.field("field", values.iterator().next());
            }
        }));

        List<IndexableField> rootFields = parsedDocument.rootDoc().getFields("field");
        List<IndexableField> prefixFields = parsedDocument.rootDoc().getFields("field._index_prefix");
        List<IndexableField> shingle2Fields = parsedDocument.rootDoc().getFields("field._2gram");
        List<IndexableField> shingle3Fields = parsedDocument.rootDoc().getFields("field._3gram");
        for (List<IndexableField> fields : List.of(rootFields, prefixFields, shingle2Fields, shingle3Fields)) {
            Set<String> expectedValues = fields.stream().map(IndexableField::stringValue).collect(Collectors.toSet());
            assertThat(values, equalTo(expectedValues));
        }
    }

    private static void assertSearchAsYouTypeFieldMapper(SearchAsYouTypeFieldMapper mapper, int maxShingleSize, String analyzerName) {

        assertThat(mapper.maxShingleSize(), equalTo(maxShingleSize));
        assertThat(mapper.fieldType(), notNullValue());
        assertSearchAsYouTypeFieldType(mapper, mapper.fieldType(), maxShingleSize, analyzerName, mapper.prefixField().fieldType());

        assertThat(mapper.prefixField(), notNullValue());
        assertThat(mapper.prefixField().fieldType().parentField, equalTo(mapper.name()));
        assertPrefixFieldType(mapper.prefixField(), mapper.indexAnalyzers(), maxShingleSize, analyzerName);

        for (int shingleSize = 2; shingleSize <= maxShingleSize; shingleSize++) {
            final ShingleFieldMapper shingleFieldMapper = mapper.shingleFields()[shingleSize - 2];
            assertThat(shingleFieldMapper, notNullValue());
            assertShingleFieldType(
                shingleFieldMapper,
                mapper.indexAnalyzers(),
                shingleSize,
                analyzerName,
                mapper.prefixField().fieldType()
            );
        }

        final int numberOfShingleSubfields = (maxShingleSize - 2) + 1;
        assertThat(mapper.shingleFields().length, equalTo(numberOfShingleSubfields));

        final Set<String> fieldsUsingSourcePath = new HashSet<>();
        mapper.sourcePathUsedBy().forEachRemaining(mapper1 -> fieldsUsingSourcePath.add(mapper1.name()));
        int multiFields = 0;
        for (FieldMapper ignored : mapper.multiFields()) {
            multiFields++;
        }
        assertThat(fieldsUsingSourcePath.size(), equalTo(numberOfShingleSubfields + 1 + multiFields));

        final Set<String> expectedFieldsUsingSourcePath = new HashSet<>();
        expectedFieldsUsingSourcePath.add(mapper.prefixField().name());
        for (ShingleFieldMapper shingleFieldMapper : mapper.shingleFields()) {
            expectedFieldsUsingSourcePath.add(shingleFieldMapper.name());
        }
        for (FieldMapper multiField : mapper.multiFields()) {
            expectedFieldsUsingSourcePath.add(multiField.name());
        }
        assertThat(fieldsUsingSourcePath, equalTo(expectedFieldsUsingSourcePath));
    }

    private static void assertSearchAsYouTypeFieldType(
        SearchAsYouTypeFieldMapper mapper,
        SearchAsYouTypeFieldType fieldType,
        int maxShingleSize,
        String analyzerName,
        PrefixFieldType prefixFieldType
    ) {

        assertThat(fieldType.shingleFields.length, equalTo(maxShingleSize - 1));
        NamedAnalyzer indexAnalyzer = mapper.indexAnalyzers().get(fieldType.name());
        for (NamedAnalyzer analyzer : asList(indexAnalyzer, fieldType.getTextSearchInfo().searchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }
        int shingleSize = 2;
        for (ShingleFieldMapper shingleField : mapper.shingleFields()) {
            assertShingleFieldType(shingleField, mapper.indexAnalyzers(), shingleSize++, analyzerName, prefixFieldType);
        }

        assertThat(fieldType.prefixField, equalTo(prefixFieldType));
    }

    private static void assertShingleFieldType(
        ShingleFieldMapper mapper,
        Map<String, NamedAnalyzer> indexAnalyzers,
        int shingleSize,
        String analyzerName,
        PrefixFieldType prefixFieldType
    ) {

        ShingleFieldType fieldType = mapper.fieldType();
        assertThat(fieldType.shingleSize, equalTo(shingleSize));

        for (NamedAnalyzer analyzer : asList(indexAnalyzers.get(fieldType.name()), fieldType.getTextSearchInfo().searchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
            if (shingleSize > 1) {
                final SearchAsYouTypeAnalyzer wrappedAnalyzer = (SearchAsYouTypeAnalyzer) analyzer.analyzer();
                assertThat(wrappedAnalyzer.shingleSize(), equalTo(shingleSize));
                assertThat(wrappedAnalyzer.indexPrefixes(), equalTo(false));
            }
        }

        assertThat(fieldType.prefixFieldType, equalTo(prefixFieldType));

    }

    private static void assertPrefixFieldType(
        PrefixFieldMapper mapper,
        Map<String, NamedAnalyzer> indexAnalyzers,
        int shingleSize,
        String analyzerName
    ) {
        PrefixFieldType fieldType = mapper.fieldType();
        NamedAnalyzer indexAnalyzer = indexAnalyzers.get(fieldType.name());
        for (NamedAnalyzer analyzer : asList(indexAnalyzer, fieldType.getTextSearchInfo().searchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }

        final SearchAsYouTypeAnalyzer wrappedIndexAnalyzer = (SearchAsYouTypeAnalyzer) indexAnalyzer.analyzer();
        final SearchAsYouTypeAnalyzer wrappedSearchAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.getTextSearchInfo()
            .searchAnalyzer()
            .analyzer();
        for (SearchAsYouTypeAnalyzer analyzer : asList(wrappedIndexAnalyzer, wrappedSearchAnalyzer)) {
            assertThat(analyzer.shingleSize(), equalTo(shingleSize));
        }
        assertThat(wrappedIndexAnalyzer.indexPrefixes(), equalTo(true));
        assertThat(wrappedSearchAnalyzer.indexPrefixes(), equalTo(false));
    }

    private static SearchAsYouTypeFieldMapper getRootFieldMapper(DocumentMapper defaultMapper, String fieldName) {
        final Mapper mapper = defaultMapper.mappers().getMapper(fieldName);
        assertThat(mapper, instanceOf(SearchAsYouTypeFieldMapper.class));
        return (SearchAsYouTypeFieldMapper) mapper;
    }

    private static ShingleFieldMapper getShingleFieldMapper(DocumentMapper defaultMapper, String fieldName) {
        final Mapper mapper = defaultMapper.mappers().getMapper(fieldName);
        assertThat(mapper, instanceOf(ShingleFieldMapper.class));
        return (ShingleFieldMapper) mapper;
    }

    private static PrefixFieldMapper getPrefixFieldMapper(DocumentMapper defaultMapper, String fieldName) {
        final Mapper mapper = defaultMapper.mappers().getMapper(fieldName);
        assertThat(mapper, instanceOf(PrefixFieldMapper.class));
        return (PrefixFieldMapper) mapper;
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("We don't have doc values or fielddata", true);
        return null;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean syntheticSource) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
