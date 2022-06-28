/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedBinaryTokenStream;
import org.apache.lucene.analysis.MockSynonymAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.search.MatchQueryParser;
import org.elasticsearch.index.search.MatchQueryParser.Type;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MatchQueryBuilderTests extends AbstractQueryTestCase<MatchQueryBuilder> {

    @Override
    protected MatchQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(
            TEXT_FIELD_NAME,
            TEXT_ALIAS_FIELD_NAME,
            BOOLEAN_FIELD_NAME,
            INT_FIELD_NAME,
            DOUBLE_FIELD_NAME,
            DATE_FIELD_NAME
        );
        Object value;
        if (isTextField(fieldName)) {
            int terms = randomIntBetween(0, 3);
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < terms; i++) {
                builder.append(randomAlphaOfLengthBetween(1, 10)).append(" ");
            }
            value = builder.toString().trim();
        } else {
            value = getRandomValueForFieldName(fieldName);
        }

        MatchQueryBuilder matchQuery = new MatchQueryBuilder(fieldName, value);
        matchQuery.operator(randomFrom(Operator.values()));

        if (randomBoolean() && isTextField(fieldName)) {
            matchQuery.analyzer(randomFrom("simple", "keyword", "whitespace"));
        }

        if (isTextField(fieldName) && randomBoolean()) {
            matchQuery.fuzziness(randomFuzziness(fieldName));
        }

        if (randomBoolean()) {
            matchQuery.prefixLength(randomIntBetween(0, 10));
        }

        if (randomBoolean()) {
            matchQuery.maxExpansions(randomIntBetween(1, 1000));
        }

        if (randomBoolean()) {
            matchQuery.minimumShouldMatch(randomMinimumShouldMatch());
        }

        if (randomBoolean()) {
            matchQuery.fuzzyRewrite(getRandomRewriteMethod());
        }

        if (randomBoolean()) {
            matchQuery.fuzzyTranspositions(randomBoolean());
        }

        if (randomBoolean()) {
            matchQuery.lenient(randomBoolean());
        }

        if (randomBoolean()) {
            matchQuery.zeroTermsQuery(randomFrom(ZeroTermsQueryOption.ALL, ZeroTermsQueryOption.NONE));
        }

        if (randomBoolean()) {
            matchQuery.autoGenerateSynonymsPhraseQuery(randomBoolean());
        }
        return matchQuery;
    }

    @Override
    protected Map<String, MatchQueryBuilder> getAlternateVersions() {
        Map<String, MatchQueryBuilder> alternateVersions = new HashMap<>();
        MatchQueryBuilder matchQuery = new MatchQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10));
        String contentString = "{\n"
            + "    \"match\" : {\n"
            + "        \""
            + matchQuery.fieldName()
            + "\" : \""
            + matchQuery.value()
            + "\"\n"
            + "    }\n"
            + "}";
        alternateVersions.put(contentString, matchQuery);
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(MatchQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, notNullValue());

        if (query instanceof MatchAllDocsQuery) {
            assertThat(queryBuilder.zeroTermsQuery(), equalTo(ZeroTermsQueryOption.ALL));
            return;
        }

        MappedFieldType fieldType = context.getFieldType(queryBuilder.fieldName());
        if (query instanceof TermQuery && fieldType != null) {
            String queryValue = queryBuilder.value().toString();
            if (isTextField(queryBuilder.fieldName()) && (queryBuilder.analyzer() == null || queryBuilder.analyzer().equals("simple"))) {
                queryValue = queryValue.toLowerCase(Locale.ROOT);
            }
            Query expectedTermQuery = fieldType.termQuery(queryValue, context);
            assertEquals(expectedTermQuery, query);
        }

        if (query instanceof BooleanQuery) {
            BooleanQuery bq = (BooleanQuery) query;
            if (queryBuilder.minimumShouldMatch() != null) {
                // calculate expected minimumShouldMatch value
                int optionalClauses = 0;
                for (BooleanClause c : bq.clauses()) {
                    if (c.getOccur() == BooleanClause.Occur.SHOULD) {
                        optionalClauses++;
                    }
                }
                int msm = Queries.calculateMinShouldMatch(optionalClauses, queryBuilder.minimumShouldMatch());
                assertThat(bq.getMinimumNumberShouldMatch(), equalTo(msm));
            }
            if (queryBuilder.analyzer() == null && queryBuilder.value().toString().length() > 0) {
                assertEquals(bq.clauses().size(), queryBuilder.value().toString().split(" ").length);
            }
        }

        if (query instanceof ExtendedCommonTermsQuery) {
            assertTrue(queryBuilder.cutoffFrequency() != null);
            ExtendedCommonTermsQuery ectq = (ExtendedCommonTermsQuery) query;
            List<Term> terms = ectq.getTerms();
            if (terms.isEmpty() == false) {
                Term term = terms.iterator().next();
                String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
                assertThat(term.field(), equalTo(expectedFieldName));
            }
            assertEquals(queryBuilder.cutoffFrequency(), ectq.getMaxTermFrequency(), Float.MIN_VALUE);
        }

        if (query instanceof FuzzyQuery) {
            assertTrue(queryBuilder.fuzziness() != null);
            FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
            // depending on analyzer being set or not we can have term lowercased along the way, so to simplify test we just
            // compare lowercased terms here
            String originalTermLc = queryBuilder.value().toString().toLowerCase(Locale.ROOT);
            String actualTermLc = fuzzyQuery.getTerm().text().toLowerCase(Locale.ROOT);
            Matcher<String> termLcMatcher = equalTo(originalTermLc);
            if ("false".equals(originalTermLc) || "true".equals(originalTermLc)) {
                // Booleans become t/f when querying a boolean field
                termLcMatcher = either(termLcMatcher).or(equalTo(originalTermLc.substring(0, 1)));
            }
            assertThat(actualTermLc, termLcMatcher);

            String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
            assertThat(expectedFieldName, equalTo(fuzzyQuery.getTerm().field()));
            assertThat(queryBuilder.prefixLength(), equalTo(fuzzyQuery.getPrefixLength()));
            assertThat(queryBuilder.fuzzyTranspositions(), equalTo(fuzzyQuery.getTranspositions()));
        }

        if (query instanceof PointRangeQuery) {
            // TODO
        }
    }

    public void testIllegalValues() {
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MatchQueryBuilder(null, "value"));
            assertEquals("[match] requires fieldName", e.getMessage());
        }

        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MatchQueryBuilder("fieldName", null));
            assertEquals("[match] requires query value", e.getMessage());
        }

        MatchQueryBuilder matchQuery = new MatchQueryBuilder("fieldName", "text");
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> matchQuery.prefixLength(-1));
            assertEquals("[match] requires prefix length to be non-negative.", e.getMessage());
        }

        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> matchQuery.maxExpansions(randomIntBetween(-10, 0))
            );
            assertEquals("[match] requires maxExpansions to be positive.", e.getMessage());
        }

        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> matchQuery.operator(null));
            assertEquals("[match] requires operator to be non-null", e.getMessage());
        }

        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> matchQuery.zeroTermsQuery(null));
            assertEquals("[match] requires zeroTermsQuery to be non-null", e.getMessage());
        }

        matchQuery.analyzer("bogusAnalyzer");
        {
            QueryShardException e = expectThrows(QueryShardException.class, () -> matchQuery.toQuery(createSearchExecutionContext()));
            assertThat(e.getMessage(), containsString("analyzer [bogusAnalyzer] not found"));
        }
    }

    public void testSimpleMatchQuery() throws IOException {
        String json = "{\n"
            + "  \"match\" : {\n"
            + "    \"message\" : {\n"
            + "      \"query\" : \"to be or not to be\",\n"
            + "      \"operator\" : \"AND\",\n"
            + "      \"prefix_length\" : 0,\n"
            + "      \"max_expansions\" : 50,\n"
            + "      \"fuzzy_transpositions\" : true,\n"
            + "      \"lenient\" : false,\n"
            + "      \"zero_terms_query\" : \"ALL\",\n"
            + "      \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}";
        MatchQueryBuilder qb = (MatchQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, qb);

        assertEquals(json, "to be or not to be", qb.value());
        assertEquals(json, Operator.AND, qb.operator());
    }

    public void testFuzzinessOnNonStringField() throws Exception {
        MatchQueryBuilder query = new MatchQueryBuilder(INT_FIELD_NAME, 42);
        query.fuzziness(randomFuzziness(INT_FIELD_NAME));
        SearchExecutionContext context = createSearchExecutionContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertEquals(
            "Can only use fuzzy queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
            e.getMessage()
        );
        query.analyzer("keyword"); // triggers a different code path
        e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertEquals(
            "Can only use fuzzy queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
            e.getMessage()
        );

        query.lenient(true);
        query.toQuery(context); // no exception
        query.analyzer(null);
        query.toQuery(context); // no exception
    }

    public void testExactOnUnsupportedField() throws Exception {
        MatchQueryBuilder query = new MatchQueryBuilder(GEO_POINT_FIELD_NAME, "2,3");
        SearchExecutionContext context = createSearchExecutionContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertEquals("Field [mapped_geo_point] of type [geo_point] does not support match queries", e.getMessage());
        query.lenient(true);
        assertThat(query.toQuery(context), Matchers.instanceOf(MatchNoDocsQuery.class));
    }

    public void testLenientFlag() throws Exception {
        MatchQueryBuilder query = new MatchQueryBuilder(BINARY_FIELD_NAME, "test");
        SearchExecutionContext context = createSearchExecutionContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertEquals("Field [mapped_binary] of type [binary] does not support match queries", e.getMessage());
        query.lenient(true);
        query.toQuery(context);
        assertThat(query.toQuery(context), Matchers.instanceOf(MatchNoDocsQuery.class));
    }

    public void testParseFailsWithMultipleFields() {
        String json = "{\n"
            + "  \"match\" : {\n"
            + "    \"message1\" : {\n"
            + "      \"query\" : \"this is a test\"\n"
            + "    },\n"
            + "    \"message2\" : {\n"
            + "      \"query\" : \"this is a test\"\n"
            + "    }\n"
            + "  }\n"
            + "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[match] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = "{\n"
            + "  \"match\" : {\n"
            + "    \"message1\" : \"this is a test\",\n"
            + "    \"message2\" : \"this is a test\"\n"
            + "  }\n"
            + "}";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[match] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }

    public void testParseFailsWithTermsArray() {
        String json1 = "{\n"
            + "  \"match\" : {\n"
            + "    \"message1\" : {\n"
            + "      \"query\" : [\"term1\", \"term2\"]\n"
            + "    }\n"
            + "  }\n"
            + "}";
        expectThrows(ParsingException.class, () -> parseQuery(json1));

        String json2 = "{\n" + "  \"match\" : {\n" + "    \"message1\" : [\"term1\", \"term2\"]\n" + "  }\n" + "}";
        expectThrows(IllegalStateException.class, () -> parseQuery(json2));
    }

    public void testExceptionUsingAnalyzerOnNumericField() {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(DOUBLE_FIELD_NAME, 6.075210893508043E-4);
        matchQueryBuilder.analyzer("simple");
        NumberFormatException e = expectThrows(NumberFormatException.class, () -> matchQueryBuilder.toQuery(searchExecutionContext));
        assertEquals("For input string: \"e\"", e.getMessage());
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(
            "_doc",
            new CompressedXContent(
                Strings.toString(
                    PutMappingRequest.buildFromSimplifiedDef(
                        "_doc",
                        "string_boost",
                        "type=text",
                        "string_no_pos",
                        "type=text,index_options=docs"
                    )
                )
            ),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    public void testMatchPhrasePrefixWithBoost() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext();
        {
            // field boost is ignored on a single term query
            MatchPhrasePrefixQueryBuilder builder = new MatchPhrasePrefixQueryBuilder("string_boost", "foo");
            Query query = builder.toQuery(context);
            assertThat(query, instanceOf(MultiPhrasePrefixQuery.class));
        }

        {
            // field boost is ignored on phrase query
            MatchPhrasePrefixQueryBuilder builder = new MatchPhrasePrefixQueryBuilder("string_boost", "foo bar");
            Query query = builder.toQuery(context);
            assertThat(query, instanceOf(MultiPhrasePrefixQuery.class));
        }
    }

    public void testLenientPhraseQuery() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext();
        MatchQueryParser b = new MatchQueryParser(context);
        b.setLenient(true);
        Query query = b.parse(Type.PHRASE, "string_no_pos", "foo bar");
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
        assertThat(query.toString(), containsString("field:[string_no_pos] was indexed without position data; cannot run PhraseQuery"));
    }

    public void testAutoGenerateSynonymsPhraseQuery() throws Exception {
        final MatchQueryParser matchQueryParser = new MatchQueryParser(createSearchExecutionContext());
        matchQueryParser.setAnalyzer(new MockSynonymAnalyzer());

        {
            matchQueryParser.setAutoGenerateSynonymsPhraseQuery(false);
            final Query query = matchQueryParser.parse(Type.BOOLEAN, TEXT_FIELD_NAME, "guinea pig");
            final Query expectedQuery = new BooleanQuery.Builder().add(
                new BooleanQuery.Builder().add(
                    new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "guinea")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term(TEXT_FIELD_NAME, "pig")), BooleanClause.Occur.MUST)
                        .build(),
                    BooleanClause.Occur.SHOULD
                ).add(new TermQuery(new Term(TEXT_FIELD_NAME, "cavy")), BooleanClause.Occur.SHOULD).build(),
                BooleanClause.Occur.SHOULD
            ).build();
            assertThat(query, equalTo(expectedQuery));
        }

        {
            matchQueryParser.setAutoGenerateSynonymsPhraseQuery(true);
            final Query query = matchQueryParser.parse(Type.BOOLEAN, TEXT_FIELD_NAME, "guinea pig");
            final Query expectedQuery = new BooleanQuery.Builder().add(
                new BooleanQuery.Builder().add(
                    new PhraseQuery.Builder().add(new Term(TEXT_FIELD_NAME, "guinea")).add(new Term(TEXT_FIELD_NAME, "pig")).build(),
                    BooleanClause.Occur.SHOULD
                ).add(new TermQuery(new Term(TEXT_FIELD_NAME, "cavy")), BooleanClause.Occur.SHOULD).build(),
                BooleanClause.Occur.SHOULD
            ).build();
            assertThat(query, equalTo(expectedQuery));
        }

        {
            matchQueryParser.setAutoGenerateSynonymsPhraseQuery(false);
            final Query query = matchQueryParser.parse(Type.BOOLEAN_PREFIX, TEXT_FIELD_NAME, "guinea pig");
            final Query expectedQuery = new BooleanQuery.Builder().add(
                new BooleanQuery.Builder().add(
                    new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "guinea")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term(TEXT_FIELD_NAME, "pig")), BooleanClause.Occur.MUST)
                        .build(),
                    BooleanClause.Occur.SHOULD
                ).add(new TermQuery(new Term(TEXT_FIELD_NAME, "cavy")), BooleanClause.Occur.SHOULD).build(),
                BooleanClause.Occur.SHOULD
            ).build();
            assertThat(query, equalTo(expectedQuery));
        }

        {
            matchQueryParser.setAutoGenerateSynonymsPhraseQuery(true);
            final Query query = matchQueryParser.parse(Type.BOOLEAN_PREFIX, TEXT_FIELD_NAME, "guinea pig");
            final MultiPhrasePrefixQuery guineaPig = new MultiPhrasePrefixQuery(TEXT_FIELD_NAME);
            guineaPig.add(new Term(TEXT_FIELD_NAME, "guinea"));
            guineaPig.add(new Term(TEXT_FIELD_NAME, "pig"));
            final MultiPhrasePrefixQuery cavy = new MultiPhrasePrefixQuery(TEXT_FIELD_NAME);
            cavy.add(new Term(TEXT_FIELD_NAME, "cavy"));
            final Query expectedQuery = new BooleanQuery.Builder().add(
                new BooleanQuery.Builder().add(guineaPig, BooleanClause.Occur.SHOULD).add(cavy, BooleanClause.Occur.SHOULD).build(),
                BooleanClause.Occur.SHOULD
            ).build();
            assertThat(query, equalTo(expectedQuery));
        }
    }

    public void testMultiWordSynonymsPhrase() throws Exception {
        final MatchQueryParser matchQueryParser = new MatchQueryParser(createSearchExecutionContext());
        matchQueryParser.setAnalyzer(new MockSynonymAnalyzer());
        final Query actual = matchQueryParser.parse(Type.PHRASE, TEXT_FIELD_NAME, "guinea pig dogs");
        Query expected = SpanNearQuery.newOrderedNearQuery(TEXT_FIELD_NAME)
            .addClause(
                new SpanOrQuery(
                    new SpanQuery[] {
                        SpanNearQuery.newOrderedNearQuery(TEXT_FIELD_NAME)
                            .addClause(new SpanTermQuery(new Term(TEXT_FIELD_NAME, "guinea")))
                            .addClause(new SpanTermQuery(new Term(TEXT_FIELD_NAME, "pig")))
                            .setSlop(0)
                            .build(),
                        new SpanTermQuery(new Term(TEXT_FIELD_NAME, "cavy")) }
                )
            )
            .addClause(
                new SpanOrQuery(
                    new SpanQuery[] {
                        new SpanTermQuery(new Term(TEXT_FIELD_NAME, "dogs")),
                        new SpanTermQuery(new Term(TEXT_FIELD_NAME, "dog")) }
                )
            )
            .build();
        assertEquals(expected, actual);
    }

    public void testAliasWithSynonyms() throws Exception {
        MatchQueryParser matchQueryParser = new MatchQueryParser(createSearchExecutionContext());
        matchQueryParser.setAnalyzer(new MockSynonymAnalyzer());

        for (Type type : Arrays.asList(Type.BOOLEAN, Type.PHRASE)) {
            Query actual = matchQueryParser.parse(type, TEXT_ALIAS_FIELD_NAME, "dogs");
            Query expected = new SynonymQuery.Builder(TEXT_FIELD_NAME).addTerm(new Term(TEXT_FIELD_NAME, "dogs"))
                .addTerm(new Term(TEXT_FIELD_NAME, "dog"))
                .build();
            assertEquals(expected, actual);
        }
    }

    public void testMaxBooleanClause() {
        MatchQueryParser query = new MatchQueryParser(createSearchExecutionContext());
        query.setAnalyzer(new MockGraphAnalyzer(createGiantGraph(40)));
        expectThrows(BooleanQuery.TooManyClauses.class, () -> query.parse(Type.PHRASE, TEXT_FIELD_NAME, ""));
        query.setAnalyzer(new MockGraphAnalyzer(createGiantGraphMultiTerms()));
        expectThrows(BooleanQuery.TooManyClauses.class, () -> query.parse(Type.PHRASE, TEXT_FIELD_NAME, ""));
    }

    private static class MockGraphAnalyzer extends Analyzer {

        CannedBinaryTokenStream tokenStream;

        MockGraphAnalyzer(CannedBinaryTokenStream.BinaryToken[] tokens) {
            this.tokenStream = new CannedBinaryTokenStream(tokens);
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            return new TokenStreamComponents(r -> {}, tokenStream);
        }
    }

    /**
     * Creates a graph token stream with 2 side paths at each position.
     **/
    private static CannedBinaryTokenStream.BinaryToken[] createGiantGraph(int numPos) {
        List<CannedBinaryTokenStream.BinaryToken> tokens = new ArrayList<>();
        BytesRef term1 = new BytesRef("foo");
        BytesRef term2 = new BytesRef("bar");
        for (int i = 0; i < numPos;) {
            if (i % 2 == 0) {
                tokens.add(new CannedBinaryTokenStream.BinaryToken(term2, 1, 1));
                tokens.add(new CannedBinaryTokenStream.BinaryToken(term1, 0, 2));
                i += 2;
            } else {
                tokens.add(new CannedBinaryTokenStream.BinaryToken(term2, 1, 1));
                i++;
            }
        }
        return tokens.toArray(new CannedBinaryTokenStream.BinaryToken[0]);
    }

    /**
     * Creates a graph token stream with {@link BooleanQuery#getMaxClauseCount()}
     * expansions at the last position.
     **/
    private static CannedBinaryTokenStream.BinaryToken[] createGiantGraphMultiTerms() {
        List<CannedBinaryTokenStream.BinaryToken> tokens = new ArrayList<>();
        BytesRef term1 = new BytesRef("foo");
        BytesRef term2 = new BytesRef("bar");
        tokens.add(new CannedBinaryTokenStream.BinaryToken(term2, 1, 1));
        tokens.add(new CannedBinaryTokenStream.BinaryToken(term1, 0, 2));
        tokens.add(new CannedBinaryTokenStream.BinaryToken(term2, 1, 1));
        tokens.add(new CannedBinaryTokenStream.BinaryToken(term2, 1, 1));
        for (int i = 0; i < BooleanQuery.getMaxClauseCount(); i++) {
            tokens.add(new CannedBinaryTokenStream.BinaryToken(term1, 0, 1));
        }
        return tokens.toArray(new CannedBinaryTokenStream.BinaryToken[0]);
    }

    /**
     * "now" on date fields should make the query non-cachable.
     */
    public void testCachingStrategiesWithNow() throws IOException {
        // if we hit a date field with "now", this should diable cachability
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(DATE_FIELD_NAME, "now");
        SearchExecutionContext context = createSearchExecutionContext();
        assert context.isCacheable();
        /*
         * We use a private rewrite context here since we want the most realistic way of asserting that we are cacheable or not. We do it
         * this way in SearchService where we first rewrite the query with a private context, then reset the context and then build the
         * actual lucene query
         */
        QueryBuilder rewritten = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(rewritten.toQuery(context));
        assertFalse("query should not be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }

    public void testRewriteToTermQueries() throws IOException {
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(KEYWORD_FIELD_NAME, "value");
        queryBuilder.boost(2f);
        SearchExecutionContext context = createSearchExecutionContext();
        QueryBuilder rewritten = queryBuilder.rewrite(context);
        assertThat(rewritten, instanceOf(TermQueryBuilder.class));
        TermQueryBuilder tqb = (TermQueryBuilder) rewritten;
        assertEquals(KEYWORD_FIELD_NAME, tqb.fieldName);
        assertEquals(new BytesRef("value"), tqb.value);
        assertThat(rewritten.boost(), equalTo(2f));
    }

    public void testRewriteToTermQueryWithAnalyzer() throws IOException {
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(TEXT_FIELD_NAME, "value");
        queryBuilder.analyzer("keyword");
        SearchExecutionContext context = createSearchExecutionContext();
        QueryBuilder rewritten = queryBuilder.rewrite(context);
        assertThat(rewritten, instanceOf(TermQueryBuilder.class));
        TermQueryBuilder tqb = (TermQueryBuilder) rewritten;
        assertEquals(TEXT_FIELD_NAME, tqb.fieldName);
        assertEquals(new BytesRef("value"), tqb.value);
    }

    public void testRewriteWithFuzziness() throws IOException {
        // If we've configured fuzziness then we can't rewrite to a term query
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(KEYWORD_FIELD_NAME, "value");
        queryBuilder.fuzziness(Fuzziness.AUTO);
        SearchExecutionContext context = createSearchExecutionContext();
        QueryBuilder rewritten = queryBuilder.rewrite(context);
        assertEquals(queryBuilder, rewritten);
    }

    public void testRewriteWithLeniency() throws IOException {
        // If we've configured leniency then we can't rewrite to a term query
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(KEYWORD_FIELD_NAME, "value");
        queryBuilder.lenient(true);
        SearchExecutionContext context = createSearchExecutionContext();
        QueryBuilder rewritten = queryBuilder.rewrite(context);
        assertEquals(queryBuilder, rewritten);
    }

    public void testRewriteIndexQueryToMatchNone() throws IOException {
        QueryBuilder query = new MatchQueryBuilder("_index", "does_not_exist");
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        QueryBuilder rewritten = query.rewrite(searchExecutionContext);
        assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
    }

    public void testRewriteIndexQueryToNotMatchNone() throws IOException {
        QueryBuilder query = new MatchQueryBuilder("_index", getIndex().getName());
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        QueryBuilder rewritten = query.rewrite(searchExecutionContext);
        assertThat(rewritten, instanceOf(MatchAllQueryBuilder.class));
    }
}
