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

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.PrefixFieldMapper;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.PrefixFieldType;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.SearchAsYouTypeAnalyzer;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.SearchAsYouTypeFieldType;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.ShingleFieldMapper;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.ShingleFieldType;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.plugins.Plugin;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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

public class SearchAsYouTypeFieldMapperTests extends FieldMapperTestCase2<SearchAsYouTypeFieldMapper.Builder> {

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("new york city");
    }

    @Before
    public void addModifiers() {
        addModifier("max_shingle_size", false, (a, b) -> {
            a.maxShingleSize(3);
            b.maxShingleSize(2);
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

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    @Override
    protected SearchAsYouTypeFieldMapper.Builder newBuilder() {
        return new SearchAsYouTypeFieldMapper.Builder("sayt")
            .indexAnalyzer(new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()))
            .searchAnalyzer(new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()))
            .searchQuoteAnalyzer(new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));
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
        return new IndexAnalyzers(
            Map.of("default", dflt, "standard", standard, "keyword", keyword, "simple", simple, "whitespace", whitespace),
            Map.of(),
            Map.of()
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
        for (String field : new String[] { "field", "field._index_prefix", "field._2gram", "field._3gram"}) {
            IndexableField[] fields = doc.rootDoc().getFields(field);
            assertEquals(1, fields.length);
            assertEquals("new york city", fields[0].stringValue());
        }
    }

   public void testDefaultConfiguration() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SearchAsYouTypeFieldMapper rootMapper = getRootFieldMapper(defaultMapper, "field");
        assertRootFieldMapper(rootMapper, 3, "default");

        PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(defaultMapper, "field._index_prefix");
        assertPrefixFieldType(prefixFieldMapper.fieldType(), 3, "default");

        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "field._2gram").fieldType(), 2, "default", prefixFieldMapper.fieldType());
        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "field._3gram").fieldType(), 3, "default", prefixFieldMapper.fieldType());
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
        assertRootFieldMapper(rootMapper, maxShingleSize, analyzerName);

        PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(defaultMapper, "field._index_prefix");
        assertPrefixFieldType(prefixFieldMapper.fieldType(), maxShingleSize, analyzerName);

        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "field._2gram").fieldType(), 2, analyzerName, prefixFieldMapper.fieldType());
        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "field._3gram").fieldType(), 3, analyzerName, prefixFieldMapper.fieldType());
        assertShingleFieldType(
            getShingleFieldMapper(defaultMapper, "field._4gram").fieldType(), 4, analyzerName, prefixFieldMapper.fieldType());
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
        assertThat(e.getMessage(), containsString("different [max_shingle_size]"));
    }

    public void testMultiFields() throws IOException {
        for (int shingleSize = 2; shingleSize < 4; shingleSize++) {
            assertMultiField(shingleSize);
        }
    }

    private void assertMultiField(int shingleSize) throws IOException {
        String path = "field.suggest";
        List<String> fields = new ArrayList<>();
        fields.add(path);
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.startObject("fields");
            {
                b.startObject("suggest").field("type", "search_as_you_type").field("max_shingle_size", shingleSize).endObject();
            }
            b.endObject();
        }));
        MappedFieldType fieldType = mapperService.fieldType(path + "._index_prefix");
        assertThat(fieldType, instanceOf(PrefixFieldType.class));
        PrefixFieldType prefixFieldType = (PrefixFieldType) fieldType;
        assertEquals(path, prefixFieldType.parentField);
        for (int i = 2; i < shingleSize; i++) {
            String name = path + "._" + i + "gram";
            fields.add(name);
            fieldType = mapperService.fieldType(name);
            assertThat(fieldType, instanceOf(ShingleFieldType.class));
            ShingleFieldType ft = (ShingleFieldType) fieldType;
            assertEquals(i, ft.shingleSize);
            assertTrue(prefixFieldType == ft.prefixFieldType);
        }

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "new york city")));
        for (String field : fields) {
            IndexableField[] indexFields = doc.rootDoc().getFields(field);
            assertEquals(1, indexFields.length);
            assertEquals("new york city", indexFields[0].stringValue());
        }
    }

    public void testIndexOptions() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "search_as_you_type").field("index_options", "offsets"))
        );

        Stream.of(
            getRootFieldMapper(mapper, "field"),
            getPrefixFieldMapper(mapper, "field._index_prefix"),
            getShingleFieldMapper(mapper, "field._2gram"),
            getShingleFieldMapper(mapper, "field._3gram")
        ).forEach(m -> assertThat("for " + m.name(),
            m.fieldType.indexOptions(), equalTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)));
    }

    public void testStore() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "search_as_you_type").field("store", true)));

        assertTrue(getRootFieldMapper(mapper, "field").fieldType.stored());
        Stream.of(
            getPrefixFieldMapper(mapper, "field._index_prefix"),
            getShingleFieldMapper(mapper, "field._2gram"),
            getShingleFieldMapper(mapper, "field._3gram")
        ).forEach(m -> assertFalse("for " + m.name(), m.fieldType.stored()));
    }

    public void testIndex() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "search_as_you_type").field("index", false)));

        Stream.of(
            getRootFieldMapper(mapper, "field"),
            getPrefixFieldMapper(mapper, "field._index_prefix"),
            getShingleFieldMapper(mapper, "field._2gram"),
            getShingleFieldMapper(mapper, "field._3gram")
        ).forEach(m -> assertThat("for " + m.name(), m.fieldType.indexOptions(), equalTo(IndexOptions.NONE)));
    }

    public void testTermVectors() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "search_as_you_type").field("term_vector", "yes")));

        Stream.of(
            getRootFieldMapper(mapper, "field"),
            getShingleFieldMapper(mapper, "field._2gram"),
            getShingleFieldMapper(mapper, "field._3gram")
        ).forEach(m -> assertTrue("for " + m.name(), m.fieldType.storeTermVectors()));

        PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(mapper, "field._index_prefix");
        assertFalse(prefixFieldMapper.fieldType.storeTermVectors());
    }

    public void testNorms() throws IOException {
        // default setting
        {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

            Stream.of(
                getRootFieldMapper(mapper, "field"),
                getShingleFieldMapper(mapper, "field._2gram"),
                getShingleFieldMapper(mapper, "field._3gram")
            ).forEach(m -> assertFalse("for " + m.name(), m.fieldType.omitNorms()));

            PrefixFieldMapper prefixFieldMapper = getPrefixFieldMapper(mapper, "field._index_prefix");
            assertTrue(prefixFieldMapper.fieldType.omitNorms());
        }

        // can disable norms on search_as_you_type fields
        {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "search_as_you_type").field("norms", false)));

            Stream.of(
                getRootFieldMapper(mapper, "field"),
                getPrefixFieldMapper(mapper, "field._index_prefix"),
                getShingleFieldMapper(mapper, "field._2gram"),
                getShingleFieldMapper(mapper, "field._3gram")
            ).forEach(m -> assertTrue("for " + m.name(), m.fieldType.omitNorms()));
        }
    }


    public void testDocumentParsingSingleValue() throws IOException {
        documentParsingTestCase(Collections.singleton(randomAlphaOfLengthBetween(5, 20)));
    }

    public void testDocumentParsingMultipleValues() throws IOException {
        documentParsingTestCase(randomUnique(() -> randomAlphaOfLengthBetween(3, 20), randomIntBetween(2, 10)));
    }

    public void testMatchPhrasePrefix() throws IOException {
        QueryShardContext queryShardContext = createQueryShardContext(createMapperService(fieldMapping(this::minimalMapping)));
        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "two words").toQuery(queryShardContext);
            Query expected = new SynonymQuery.Builder("field._index_prefix").addTerm(new Term("field._index_prefix", "two words")).build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "three words here").toQuery(queryShardContext);
            Query expected = new SynonymQuery.Builder("field._index_prefix").addTerm(new Term("field._index_prefix", "three words here"))
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
            Query q = new MatchPhrasePrefixQueryBuilder("field", "more than three words").toQuery(queryShardContext);
            Query expected = new SpanNearQuery.Builder("field._3gram", true)
                .addClause(new SpanTermQuery(new Term("field._3gram", "more than three")))
                .addClause(new FieldMaskingSpanQuery(
                    new SpanTermQuery(new Term("field._index_prefix", "than three words")), "field._3gram")
                )
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field._3gram", "more than three words").toQuery(queryShardContext);
            Query expected = new SpanNearQuery.Builder("field._3gram", true)
                .addClause(new SpanTermQuery(new Term("field._3gram", "more than three")))
                .addClause(new FieldMaskingSpanQuery(
                    new SpanTermQuery(new Term("field._index_prefix", "than three words")), "field._3gram")
                )
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field._3gram", "two words").toQuery(queryShardContext);
            Query expected = new MatchNoDocsQuery();
            assertThat(q, equalTo(expected));
        }

        {
            Query actual = new MatchPhrasePrefixQueryBuilder("field._3gram", "one two three four")
                .slop(1)
                .toQuery(queryShardContext);
            MultiPhrasePrefixQuery expected = new MultiPhrasePrefixQuery("field._3gram");
            expected.setSlop(1);
            expected.add(new Term("field._3gram", "one two three"));
            expected.add(new Term("field._3gram", "two three four"));
            assertThat(actual, equalTo(expected));
        }

    }

    public void testMatchPhrase() throws IOException {
        QueryShardContext queryShardContext = createQueryShardContext(createMapperService(fieldMapping(this::minimalMapping)));
        {
            Query actual = new MatchPhraseQueryBuilder("field", "one")
                .toQuery(queryShardContext);
            Query expected = new TermQuery(new Term("field", "one"));
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field", "one two")
                .toQuery(queryShardContext);
            Query expected = new MultiPhraseQuery.Builder()
                .add(new Term("field._2gram", "one two"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field", "one two three")
                .toQuery(queryShardContext);
            Query expected = new MultiPhraseQuery.Builder()
                .add(new Term("field._3gram", "one two three"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field", "one two three four")
                .toQuery(queryShardContext);
            Query expected = new MultiPhraseQuery.Builder()
                .add(new Term("field._3gram", "one two three"))
                .add(new Term("field._3gram", "two three four"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field", "one two")
                .slop(1)
                .toQuery(queryShardContext);
            Query expected = new MultiPhraseQuery.Builder()
                .add(new Term("field", "one"))
                .add(new Term("field", "two"))
                .setSlop(1)
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field._2gram", "one two")
                .toQuery(queryShardContext);
            Query expected = new TermQuery(new Term("field._2gram", "one two"));
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field._2gram", "one two three")
                .toQuery(queryShardContext);
            Query expected = new MultiPhraseQuery.Builder()
                .add(new Term("field._2gram", "one two"))
                .add(new Term("field._2gram", "two three"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field._3gram", "one two three")
                .toQuery(queryShardContext);
            Query expected = new TermQuery(new Term("field._3gram", "one two three"));
            assertThat(actual, equalTo(expected));
        }

        {
            Query actual = new MatchPhraseQueryBuilder("field._3gram", "one two three four")
                .toQuery(queryShardContext);
            Query expected = new MultiPhraseQuery.Builder()
                .add(new Term("field._3gram", "one two three"))
                .add(new Term("field._3gram", "two three four"))
                .build();
            assertThat(actual, equalTo(expected));
        }

        {
            expectThrows(IllegalArgumentException.class,
                () -> new MatchPhraseQueryBuilder("field._index_prefix", "one two three four").toQuery(queryShardContext));
        }
    }

    private static BooleanQuery buildBoolPrefixQuery(String shingleFieldName, String prefixFieldName, List<String> terms) {
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (int i = 0; i < terms.size() - 1; i++) {
            final String term = terms.get(i);
            builder.add(new BooleanClause(new TermQuery(new Term(shingleFieldName, term)), BooleanClause.Occur.SHOULD));
        }
        final String finalTerm = terms.get(terms.size() - 1);
        builder.add(new BooleanClause(
            new ConstantScoreQuery(new TermQuery(new Term(prefixFieldName, finalTerm))), BooleanClause.Occur.SHOULD));
        return builder.build();
    }

    public void testMultiMatchBoolPrefix() throws IOException {
        QueryShardContext queryShardContext = createQueryShardContext(
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

        final Query actual = builder.toQuery(queryShardContext);
        assertThat(actual, instanceOf(DisjunctionMaxQuery.class));
        final DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) actual;
        assertThat(disMaxQuery.getDisjuncts(), hasSize(4));
        assertThat(disMaxQuery.getDisjuncts(), containsInAnyOrder(
            buildBoolPrefixQuery(
                "field", "field._index_prefix", asList("quick", "brown", "fox", "jump", "lazy", "dog")),
            buildBoolPrefixQuery("field._2gram", "field._index_prefix",
                asList("quick brown", "brown fox", "fox jump", "jump lazy", "lazy dog")),
            buildBoolPrefixQuery("field._3gram", "field._index_prefix",
                asList("quick brown fox", "brown fox jump", "fox jump lazy", "jump lazy dog")),
            buildBoolPrefixQuery("field._4gram", "field._index_prefix",
                asList("quick brown fox jump", "brown fox jump lazy", "fox jump lazy dog"))));
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

        IndexableField[] rootFields = parsedDocument.rootDoc().getFields("field");
        IndexableField[] prefixFields = parsedDocument.rootDoc().getFields("field._index_prefix");
        IndexableField[] shingle2Fields = parsedDocument.rootDoc().getFields("field._2gram");
        IndexableField[] shingle3Fields = parsedDocument.rootDoc().getFields("field._3gram");
        for (IndexableField[] fields : new IndexableField[][]{ rootFields, prefixFields, shingle2Fields, shingle3Fields}) {
            Set<String> expectedValues = Arrays.stream(fields).map(IndexableField::stringValue).collect(Collectors.toSet());
            assertThat(values, equalTo(expectedValues));
        }
    }

    private static void assertRootFieldMapper(SearchAsYouTypeFieldMapper mapper,
                                              int maxShingleSize,
                                              String analyzerName) {

        assertThat(mapper.maxShingleSize(), equalTo(maxShingleSize));
        assertThat(mapper.fieldType(), notNullValue());
        assertSearchAsYouTypeFieldType(mapper.fieldType(), maxShingleSize, analyzerName, mapper.prefixField().fieldType());

        assertThat(mapper.prefixField(), notNullValue());
        assertThat(mapper.prefixField().fieldType().parentField, equalTo(mapper.name()));
        assertPrefixFieldType(mapper.prefixField().fieldType(), maxShingleSize, analyzerName);


        for (int shingleSize = 2; shingleSize <= maxShingleSize; shingleSize++) {
            final ShingleFieldMapper shingleFieldMapper = mapper.shingleFields()[shingleSize - 2];
            assertThat(shingleFieldMapper, notNullValue());
            assertShingleFieldType(shingleFieldMapper.fieldType(), shingleSize, analyzerName, mapper.prefixField().fieldType());
        }

        final int numberOfShingleSubfields = (maxShingleSize - 2) + 1;
        assertThat(mapper.shingleFields().length, equalTo(numberOfShingleSubfields));
    }

    private static void assertSearchAsYouTypeFieldType(SearchAsYouTypeFieldType fieldType, int maxShingleSize,
                                                       String analyzerName,
                                                       PrefixFieldType prefixFieldType) {

        assertThat(fieldType.shingleFields.length, equalTo(maxShingleSize-1));
        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.getTextSearchInfo().getSearchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }
        int shingleSize = 2;
        for (ShingleFieldType shingleField : fieldType.shingleFields) {
            assertShingleFieldType(shingleField, shingleSize++, analyzerName, prefixFieldType);
        }

        assertThat(fieldType.prefixField, equalTo(prefixFieldType));
    }

    private static void assertShingleFieldType(ShingleFieldType fieldType,
                                               int shingleSize,
                                               String analyzerName,
                                               PrefixFieldType prefixFieldType) {

        assertThat(fieldType.shingleSize, equalTo(shingleSize));

        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.getTextSearchInfo().getSearchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
            if (shingleSize > 1) {
                final SearchAsYouTypeAnalyzer wrappedAnalyzer = (SearchAsYouTypeAnalyzer) analyzer.analyzer();
                assertThat(wrappedAnalyzer.shingleSize(), equalTo(shingleSize));
                assertThat(wrappedAnalyzer.indexPrefixes(), equalTo(false));
            }
        }

        assertThat(fieldType.prefixFieldType, equalTo(prefixFieldType));

    }

    private static void assertPrefixFieldType(PrefixFieldType fieldType, int shingleSize, String analyzerName) {
        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.getTextSearchInfo().getSearchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }

        final SearchAsYouTypeAnalyzer wrappedIndexAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.indexAnalyzer().analyzer();
        final SearchAsYouTypeAnalyzer wrappedSearchAnalyzer
            = (SearchAsYouTypeAnalyzer) fieldType.getTextSearchInfo().getSearchAnalyzer().analyzer();
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
}
