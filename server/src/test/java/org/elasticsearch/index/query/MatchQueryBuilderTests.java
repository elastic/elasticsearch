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

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedBinaryTokenStream;
import org.apache.lucene.analysis.MockSynonymAnalyzer;
import org.apache.lucene.index.Term;
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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.index.search.MatchQuery.Type;
import org.elasticsearch.index.search.MatchQuery.ZeroTermsQuery;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
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
        String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME, BOOLEAN_FIELD_NAME, INT_FIELD_NAME,
            DOUBLE_FIELD_NAME, DATE_FIELD_NAME);
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
            matchQuery.zeroTermsQuery(randomFrom(ZeroTermsQuery.ALL, ZeroTermsQuery.NONE));
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
        String contentString = "{\n" +
            "    \"match\" : {\n" +
            "        \"" + matchQuery.fieldName() + "\" : \"" + matchQuery.value() + "\"\n" +
            "    }\n" +
            "}";
        alternateVersions.put(contentString, matchQuery);
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(MatchQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, notNullValue());

        if (query instanceof MatchAllDocsQuery) {
            assertThat(queryBuilder.zeroTermsQuery(), equalTo(ZeroTermsQuery.ALL));
            return;
        }

        MappedFieldType fieldType = context.fieldMapper(queryBuilder.fieldName());
        if (query instanceof TermQuery && fieldType != null) {
            String queryValue = queryBuilder.value().toString();
            if (isTextField(queryBuilder.fieldName())
                  && (queryBuilder.analyzer() == null || queryBuilder.analyzer().equals("simple"))) {
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
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> matchQuery.maxExpansions(randomIntBetween(-10, 0)));
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
            QueryShardException e = expectThrows(QueryShardException.class, () -> matchQuery.toQuery(createShardContext()));
            assertThat(e.getMessage(), containsString("analyzer [bogusAnalyzer] not found"));
        }
    }

    public void testSimpleMatchQuery() throws IOException {
        String json = "{\n" +
            "  \"match\" : {\n" +
            "    \"message\" : {\n" +
            "      \"query\" : \"to be or not to be\",\n" +
            "      \"operator\" : \"AND\",\n" +
            "      \"prefix_length\" : 0,\n" +
            "      \"max_expansions\" : 50,\n" +
            "      \"fuzzy_transpositions\" : true,\n" +
            "      \"lenient\" : false,\n" +
            "      \"zero_terms_query\" : \"ALL\",\n" +
            "      \"auto_generate_synonyms_phrase_query\" : true,\n" +
            "      \"boost\" : 1.0\n" +
            "    }\n" +
            "  }\n" +
            "}";
        MatchQueryBuilder qb = (MatchQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, qb);

        assertEquals(json, "to be or not to be", qb.value());
        assertEquals(json, Operator.AND, qb.operator());
    }

    public void testFuzzinessOnNonStringField() throws Exception {
        MatchQueryBuilder query = new MatchQueryBuilder(INT_FIELD_NAME, 42);
        query.fuzziness(randomFuzziness(INT_FIELD_NAME));
        QueryShardContext context = createShardContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> query.toQuery(context));
        assertEquals("Can only use fuzzy queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
            e.getMessage());
        query.analyzer("keyword"); // triggers a different code path
        e = expectThrows(IllegalArgumentException.class,
            () -> query.toQuery(context));
        assertEquals("Can only use fuzzy queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
            e.getMessage());

        query.lenient(true);
        query.toQuery(context); // no exception
        query.analyzer(null);
        query.toQuery(context); // no exception
    }

    public void testExactOnUnsupportedField() throws Exception {
        MatchQueryBuilder query = new MatchQueryBuilder(GEO_POINT_FIELD_NAME, "2,3");
        QueryShardContext context = createShardContext();
        QueryShardException e = expectThrows(QueryShardException.class, () -> query.toQuery(context));
        assertEquals("Geometry fields do not support exact searching, use dedicated geometry queries instead: " +
            "[mapped_geo_point]", e.getMessage());
        query.lenient(true);
        query.toQuery(context); // no exception
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = "{\n" +
            "  \"match\" : {\n" +
            "    \"message1\" : {\n" +
            "      \"query\" : \"this is a test\"\n" +
            "    },\n" +
            "    \"message2\" : {\n" +
            "      \"query\" : \"this is a test\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[match] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = "{\n" +
            "  \"match\" : {\n" +
            "    \"message1\" : \"this is a test\",\n" +
            "    \"message2\" : \"this is a test\"\n" +
            "  }\n" +
            "}";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[match] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }

    public void testParseFailsWithTermsArray() throws Exception {
        String json1 = "{\n" +
            "  \"match\" : {\n" +
            "    \"message1\" : {\n" +
            "      \"query\" : [\"term1\", \"term2\"]\n" +
            "    }\n" +
            "  }\n" +
            "}";
        expectThrows(ParsingException.class, () -> parseQuery(json1));

        String json2 = "{\n" +
            "  \"match\" : {\n" +
            "    \"message1\" : [\"term1\", \"term2\"]\n" +
            "  }\n" +
            "}";
        expectThrows(IllegalStateException.class, () -> parseQuery(json2));
    }

    public void testExceptionUsingAnalyzerOnNumericField() {
        QueryShardContext shardContext = createShardContext();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(DOUBLE_FIELD_NAME, 6.075210893508043E-4);
        matchQueryBuilder.analyzer("simple");
        NumberFormatException e = expectThrows(NumberFormatException.class, () -> matchQueryBuilder.toQuery(shardContext));
        assertEquals("For input string: \"e\"", e.getMessage());
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge("_doc", new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(
            "string_boost", "type=text,boost=4", "string_no_pos",
            "type=text,index_options=docs"))
            ),
            MapperService.MergeReason.MAPPING_UPDATE);
    }

    public void testMatchPhrasePrefixWithBoost() throws Exception {
        QueryShardContext context = createShardContext();
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
        QueryShardContext context = createShardContext();
        MatchQuery b = new MatchQuery(context);
        b.setLenient(true);
        Query query = b.parse(Type.PHRASE, "string_no_pos", "foo bar");
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
        assertThat(query.toString(),
            containsString("field:[string_no_pos] was indexed without position data; cannot run PhraseQuery"));
    }

    public void testAutoGenerateSynonymsPhraseQuery() throws Exception {
        final MatchQuery matchQuery = new MatchQuery(createShardContext());
        matchQuery.setAnalyzer(new MockSynonymAnalyzer());

        {
            matchQuery.setAutoGenerateSynonymsPhraseQuery(false);
            final Query query = matchQuery.parse(Type.BOOLEAN, TEXT_FIELD_NAME, "guinea pig");
            final Query expectedQuery = new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                        .add(new BooleanQuery.Builder()
                                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "guinea")), BooleanClause.Occur.MUST)
                                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "pig")), BooleanClause.Occur.MUST)
                                .build(),
                            BooleanClause.Occur.SHOULD)
                        .add(new TermQuery(new Term(TEXT_FIELD_NAME, "cavy")), BooleanClause.Occur.SHOULD)
                        .build(),
                    BooleanClause.Occur.SHOULD).build();
            assertThat(query, equalTo(expectedQuery));
        }

        {
            matchQuery.setAutoGenerateSynonymsPhraseQuery(true);
            final Query query = matchQuery.parse(Type.BOOLEAN, TEXT_FIELD_NAME, "guinea pig");
            final Query expectedQuery = new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                        .add(new PhraseQuery.Builder()
                            .add(new Term(TEXT_FIELD_NAME, "guinea"))
                            .add(new Term(TEXT_FIELD_NAME, "pig"))
                            .build(),
                            BooleanClause.Occur.SHOULD)
                        .add(new TermQuery(new Term(TEXT_FIELD_NAME, "cavy")), BooleanClause.Occur.SHOULD)
                        .build(),
                    BooleanClause.Occur.SHOULD).build();
            assertThat(query, equalTo(expectedQuery));
        }

        {
            matchQuery.setAutoGenerateSynonymsPhraseQuery(false);
            final Query query = matchQuery.parse(Type.BOOLEAN_PREFIX, TEXT_FIELD_NAME, "guinea pig");
            final Query expectedQuery = new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                        .add(new BooleanQuery.Builder()
                                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "guinea")), BooleanClause.Occur.MUST)
                                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "pig")), BooleanClause.Occur.MUST)
                                .build(),
                            BooleanClause.Occur.SHOULD)
                        .add(new TermQuery(new Term(TEXT_FIELD_NAME, "cavy")), BooleanClause.Occur.SHOULD)
                        .build(),
                    BooleanClause.Occur.SHOULD).build();
            assertThat(query, equalTo(expectedQuery));
        }

        {
            matchQuery.setAutoGenerateSynonymsPhraseQuery(true);
            final Query query = matchQuery.parse(Type.BOOLEAN_PREFIX, TEXT_FIELD_NAME, "guinea pig");
            final MultiPhrasePrefixQuery guineaPig = new MultiPhrasePrefixQuery(TEXT_FIELD_NAME);
            guineaPig.add(new Term(TEXT_FIELD_NAME, "guinea"));
            guineaPig.add(new Term(TEXT_FIELD_NAME, "pig"));
            final MultiPhrasePrefixQuery cavy = new MultiPhrasePrefixQuery(TEXT_FIELD_NAME);
            cavy.add(new Term(TEXT_FIELD_NAME, "cavy"));
            final Query expectedQuery = new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                        .add(guineaPig, BooleanClause.Occur.SHOULD)
                        .add(cavy, BooleanClause.Occur.SHOULD)
                        .build(),
                    BooleanClause.Occur.SHOULD).build();
            assertThat(query, equalTo(expectedQuery));
        }
    }

    public void testMultiWordSynonymsPhrase() throws Exception {
        final MatchQuery matchQuery = new MatchQuery(createShardContext());
        matchQuery.setAnalyzer(new MockSynonymAnalyzer());
        final Query actual = matchQuery.parse(Type.PHRASE, TEXT_FIELD_NAME, "guinea pig dogs");
        Query expected = SpanNearQuery.newOrderedNearQuery(TEXT_FIELD_NAME)
            .addClause(
                new SpanOrQuery(new SpanQuery[]{
                    SpanNearQuery.newOrderedNearQuery(TEXT_FIELD_NAME)
                        .addClause(new SpanTermQuery(new Term(TEXT_FIELD_NAME, "guinea")))
                        .addClause(new SpanTermQuery(new Term(TEXT_FIELD_NAME, "pig")))
                        .setSlop(0)
                        .build(),
                    new SpanTermQuery(new Term(TEXT_FIELD_NAME, "cavy"))
                })
            )
            .addClause(new SpanOrQuery(new SpanQuery[]{
                new SpanTermQuery(new Term(TEXT_FIELD_NAME, "dogs")),
                new SpanTermQuery(new Term(TEXT_FIELD_NAME, "dog"))
            }))
            .build();
        assertEquals(expected, actual);
    }


    public void testAliasWithSynonyms() throws Exception {
        final MatchQuery matchQuery = new MatchQuery(createShardContext());
        matchQuery.setAnalyzer(new MockSynonymAnalyzer());
        final Query actual = matchQuery.parse(Type.PHRASE, TEXT_ALIAS_FIELD_NAME, "dogs");
        Query expected = new SynonymQuery.Builder(TEXT_FIELD_NAME)
            .addTerm(new Term(TEXT_FIELD_NAME, "dogs"))
            .addTerm(new Term(TEXT_FIELD_NAME, "dog"))
            .build();
        assertEquals(expected, actual);
    }

    public void testMaxBooleanClause() {
        MatchQuery query = new MatchQuery(createShardContext());
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
        QueryShardContext context = createShardContext();
        assert context.isCacheable();
        /*
         * We use a private rewrite context here since we want the most realistic way of asserting that we are cacheable or not. We do it
         * this way in SearchService where we first rewrite the query with a private context, then reset the context and then build the
         * actual lucene query
         */
        QueryBuilder rewritten = rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(rewritten.toQuery(context));
        assertFalse("query should not be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }
}
