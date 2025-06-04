/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;
import static org.elasticsearch.search.SearchModule.INDICES_MAX_NESTED_DEPTH_SETTING;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public abstract class AbstractQueryTestCase<QB extends AbstractQueryBuilder<QB>> extends AbstractBuilderTestCase {

    private static final int NUMBER_OF_TESTQUERIES = 20;

    public final QB createTestQueryBuilder() {
        return createTestQueryBuilder(supportsBoost(), supportsQueryName());
    }

    public final QB createTestQueryBuilder(boolean supportsBoost, boolean supportsQueryName) {
        QB query = doCreateTestQueryBuilder();
        if (supportsBoost && randomBoolean()) {
            query.boost(2.0f / randomIntBetween(1, 20));
        }
        if (supportsQueryName && randomBoolean()) {
            query.queryName(createUniqueRandomName());
        }
        return query;
    }

    /**
     * Create the query that is being tested
     */
    protected abstract QB doCreateTestQueryBuilder();

    /**
     * Create the query that is being tested holding the provided inner query.
     * To be overridden only for queries that support inner queries.
     */
    protected QB createQueryWithInnerQuery(QueryBuilder queryBuilder) {
        throw new UnsupportedOperationException();
    }

    public void testMaxNestedDepth() throws IOException {
        QB query = null;
        try {
            query = createQueryWithInnerQuery(new MatchAllQueryBuilder());
        } catch (UnsupportedOperationException e) {
            assumeNoException("Runs only for queries that support nesting", e);
        }
        int maxDepth = randomIntBetween(3, 5);
        AbstractQueryBuilder.setMaxNestedDepth(maxDepth);
        try {
            for (int i = 1; i < maxDepth - 1; i++) {
                query = createQueryWithInnerQuery(query);
            }
            // no errors, we reached the limit but we did not go beyond it
            parseQuery(Strings.toString(query));
            String expectedMessage = "The nested depth of the query exceeds the maximum nested depth for queries set in ["
                + INDICES_MAX_NESTED_DEPTH_SETTING.getKey()
                + "]";
            QB q = query;
            // one more level causes an exception
            Exception exception = expectThrows(Exception.class, () -> parseQuery(Strings.toString(createQueryWithInnerQuery(q))));
            // there may be nested XContentParseExceptions coming from ObjectParser, we just extract the root cause
            while (exception.getCause() != null) {
                assertThat(exception.getCause(), either(instanceOf(IllegalArgumentException.class)).or(instanceOf(ParsingException.class)));
                exception = (Exception) exception.getCause();
            }
            assertEquals(expectedMessage, exception.getMessage());
        } finally {
            AbstractQueryBuilder.setMaxNestedDepth(INDICES_MAX_NESTED_DEPTH_SETTING.getDefault(Settings.EMPTY));
        }
    }

    public void testNegativeBoosts() {
        QB testQuery = createTestQueryBuilder();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> testQuery.boost(-0.5f));
        assertThat(exc.getMessage(), containsString("negative [boost]"));
    }

    /**
     * Generic test that creates new query from the test query and checks both for equality
     * and asserts equality on the two queries.
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTQUERIES; runs++) {
            QB testQuery = createTestQueryBuilder();
            XContentType xContentType = randomFrom(XContentType.values());
            BytesReference shuffledXContent = toShuffledXContent(
                testQuery,
                xContentType,
                ToXContent.EMPTY_PARAMS,
                randomBoolean(),
                shuffleProtectedFields()
            );
            try (var parser = createParser(xContentType.xContent(), shuffledXContent)) {
                assertParsedQuery(parser, testQuery);
            }
            for (Map.Entry<String, QB> alternateVersion : getAlternateVersions().entrySet()) {
                String queryAsString = alternateVersion.getKey();
                try (var parser = createParser(JsonXContent.jsonXContent, queryAsString)) {
                    assertParsedQuery(parser, alternateVersion.getValue());
                }
            }
        }
    }

    /**
     * Subclasses can override this method and return an array of fieldnames which should be protected from
     * recursive random shuffling in the {@link #testFromXContent()} test case
     */
    protected String[] shuffleProtectedFields() {
        return Strings.EMPTY_ARRAY;
    }

    /**
     * Test that unknown field trigger ParsingException.
     * To find the right position in the root query, we add a marker as `queryName` which
     * all query builders support. The added bogus field after that should trigger the exception.
     * Queries that allow arbitrary field names at this level need to override this test.
     */
    public void testUnknownField() throws IOException {
        String marker = "#marker#";
        QB testQuery;
        do {
            testQuery = createTestQueryBuilder();
        } while (testQuery.toString().contains(marker));
        testQuery.queryName(marker); // to find root query to add additional bogus field there
        String queryAsString = testQuery.toString().replace("\"" + marker + "\"", "\"" + marker + "\", \"bogusField\" : \"someValue\"");
        try {
            parseQuery(queryAsString);
            fail("expected ParsingException or XContentParsingException");
        } catch (ParsingException | XContentParseException e) {
            // we'd like to see the offending field name here
            assertThat(e.getMessage(), containsString("bogusField"));
        }

    }

    /**
     * Test that adding an additional object within each object of the otherwise correct query always triggers some kind of
     * parse exception. Some specific objects do not cause any exception as they can hold arbitrary content; they can be
     * declared by overriding {@link #getObjectsHoldingArbitraryContent()}.
     */
    public void testUnknownObjectException() throws IOException {
        Set<String> candidates = new HashSet<>();
        // Adds the valid query to the list of queries to modify and test
        candidates.add(createTestQueryBuilder().toString());
        // Adds the alternates versions of the query too
        candidates.addAll(getAlternateVersions().keySet());

        List<Tuple<String, String>> testQueries = alterateQueries(candidates, getObjectsHoldingArbitraryContent());
        for (Tuple<String, String> testQuery : testQueries) {
            String expectedException = testQuery.v2();
            try {
                parseQuery(testQuery.v1());
                if (expectedException != null) {
                    fail("some parsing exception expected for query: " + testQuery);
                }
            } catch (ParsingException | ElasticsearchParseException | XContentParseException e) {
                // different kinds of exception wordings depending on location
                // of mutation, so no simple asserts possible here
                if (expectedException == null) {
                    throw new AssertionError("unexpected exception when parsing query:\n" + testQuery, e);
                }
            } catch (IllegalArgumentException e) {
                if (expectedException == null) {
                    throw new AssertionError("unexpected exception when parsing query:\n" + testQuery, e);
                }
                assertThat(e.getMessage(), containsString(expectedException));
            }
        }
    }

    /**
     * Traverses the json tree of the valid query provided as argument and mutates it one or more times by adding one object within each
     * object encountered.
     *
     * For instance given the following valid term query:
     * {
     *     "term" : {
     *         "field" : {
     *             "value" : "foo"
     *         }
     *     }
     * }
     *
     * The following two mutations will be generated, and an exception is expected when trying to parse them:
     * {
     *     "term" : {
     *         "newField" : {
     *             "field" : {
     *                 "value" : "foo"
     *             }
     *         }
     *     }
     * }
     *
     * {
     *     "term" : {
     *         "field" : {
     *             "newField" : {
     *                 "value" : "foo"
     *             }
     *         }
     *     }
     * }
     *
     * Every mutation is then added to the list of results with a boolean flag indicating if a parsing exception is expected or not
     * for the mutation. Some specific objects do not cause any exception as they can hold arbitrary content; they are passed using the
     * arbitraryMarkers parameter.
     */
    static List<Tuple<String, String>> alterateQueries(Set<String> queries, Map<String, String> arbitraryMarkers) throws IOException {
        List<Tuple<String, String>> results = new ArrayList<>();

        // Indicate if a part of the query can hold any arbitrary content
        boolean hasArbitraryContent = (arbitraryMarkers != null && arbitraryMarkers.isEmpty() == false);

        for (String query : queries) {
            // Track the number of query mutations
            int mutation = 0;

            while (true) {
                String exception = "unknown field [newField]";

                BytesStreamOutput out = new BytesStreamOutput();
                try (
                    XContentGenerator generator = XContentType.JSON.xContent().createGenerator(out);
                    XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, query)
                ) {
                    int objectIndex = -1;
                    Deque<String> levels = new LinkedList<>();

                    // Parse the valid query and inserts a new object level called "newField"
                    XContentParser.Token token;
                    while ((token = parser.nextToken()) != null) {
                        if (token == XContentParser.Token.START_ARRAY) {
                            levels.addLast(parser.currentName());
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            objectIndex++;
                            levels.addLast(parser.currentName());

                            if (objectIndex == mutation) {
                                // We reached the place in the object tree where we want to insert a new object level
                                generator.writeStartObject();
                                generator.writeFieldName("newField");
                                generator.copyCurrentStructure(parser);
                                generator.writeEndObject();

                                if (hasArbitraryContent) {
                                    // The query has one or more fields that hold arbitrary content. If the current
                                    // field is one (or a child) of those, no exception is expected when parsing the mutated query.
                                    for (String marker : arbitraryMarkers.keySet()) {
                                        if (levels.contains(marker)) {
                                            exception = arbitraryMarkers.get(marker);
                                            break;
                                        }
                                    }
                                }

                                // Jump to next token
                                continue;
                            }
                        } else if (token == XContentParser.Token.END_OBJECT || token == XContentParser.Token.END_ARRAY) {
                            levels.removeLast();
                        }

                        // We are walking through the object tree, so we can safely copy the current node
                        generator.copyCurrentEvent(parser);
                    }

                    if (objectIndex < mutation) {
                        // We did not reach the insertion point, there's no more mutations to try
                        break;
                    } else {
                        // We reached the expected insertion point, so next time we'll try one step further
                        mutation++;
                    }
                }

                results.add(new Tuple<>(out.bytes().utf8ToString(), exception));
            }
        }
        return results;
    }

    /**
     * Returns a map where the keys are object names that won't trigger a standard exception (an exception that contains the string
     * "unknown field [newField]") through {@link #testUnknownObjectException()}. The value is a string that is contained in the thrown
     * exception or null in the case that no exception is thrown (including their children).
     * Default is an empty Map. Can be overridden by subclasses that test queries which contain objects that get parsed on the data nodes
     * (e.g. score functions) or objects that can contain arbitrary content (e.g. documents for percolate or more like this query, params
     * for scripts) and/or expect some content(e.g documents with geojson geometries).
     */
    protected Map<String, String> getObjectsHoldingArbitraryContent() {
        return Collections.emptyMap();
    }

    /**
     * Test that wraps the randomly generated query into an array as follows: { "query_name" : [{}]}
     * This causes unexpected situations in parser code that may not be handled properly.
     */
    public final void testQueryWrappedInArray() {
        QB queryBuilder = createTestQueryBuilder();
        String queryName = queryBuilder.getName();
        String validQuery = queryBuilder.toString();
        queryWrappedInArrayTest(queryName, validQuery);
        for (String query : getAlternateVersions().keySet()) {
            queryWrappedInArrayTest(queryName, query);
        }
    }

    private void queryWrappedInArrayTest(String queryName, String validQuery) {
        int i = validQuery.indexOf("\"" + queryName + "\"");
        assertThat(i, greaterThan(0));

        int insertionPosition;
        for (insertionPosition = i; insertionPosition < validQuery.length(); insertionPosition++) {
            if (validQuery.charAt(insertionPosition) == ':') {
                break;
            }
        }
        insertionPosition++;

        int endArrayPosition;
        for (endArrayPosition = validQuery.length() - 1; endArrayPosition >= 0; endArrayPosition--) {
            if (validQuery.charAt(endArrayPosition) == '}') {
                break;
            }
        }

        String testQuery = validQuery.substring(0, insertionPosition)
            + "["
            + validQuery.substring(insertionPosition, endArrayPosition)
            + "]"
            + validQuery.substring(endArrayPosition);

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(testQuery));
        assertEquals("[" + queryName + "] query malformed, no start_object after query name", e.getMessage());
    }

    /**
     * Returns alternate string representation of the query that need to be tested as they are never used as output
     * of {@link QueryBuilder#toXContent(XContentBuilder, ToXContent.Params)}. By default there are no alternate versions.
     */
    protected Map<String, QB> getAlternateVersions() {
        return Collections.emptyMap();
    }

    /**
     * Parses the query provided as string argument and compares it with the expected result provided as argument as a {@link QueryBuilder}
     */
    protected void assertParsedQuery(String queryAsString, QueryBuilder expectedQuery) throws IOException {
        QueryBuilder newQuery = parseQuery(queryAsString);
        assertNotSame(newQuery, expectedQuery);
        assertEquals(expectedQuery, newQuery);
        assertEquals(expectedQuery.hashCode(), newQuery.hashCode());
    }

    /**
     * Parses the query provided as bytes argument and compares it with the expected result provided as argument as a {@link QueryBuilder}
     */
    private void assertParsedQuery(XContentParser parser, QueryBuilder expectedQuery) throws IOException {
        QueryBuilder newQuery = parseQuery(parser);
        assertNotSame(newQuery, expectedQuery);
        assertEquals(expectedQuery, newQuery);
        assertEquals(expectedQuery.hashCode(), newQuery.hashCode());
    }

    protected QueryBuilder parseQuery(AbstractQueryBuilder<?> builder) throws IOException {
        BytesReference bytes = XContentHelper.toXContent(builder, XContentType.JSON, false);
        try (var parser = createParser(JsonXContent.jsonXContent, bytes)) {
            return parseQuery(parser);
        }
    }

    protected QueryBuilder parseQuery(String queryAsString) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, queryAsString)) {
            return parseQuery(parser);
        }
    }

    protected QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder parseInnerQueryBuilder = parseTopLevelQuery(parser);
        assertNull(parser.nextToken());
        return parseInnerQueryBuilder;
    }

    /**
     * Whether the queries produced by this builder are expected to be cacheable.
     */
    protected boolean builderGeneratesCacheableQueries() {
        return true;
    }

    /**
     * Test creates the {@link Query} from the {@link QueryBuilder} under test and delegates the
     * assertions being made on the result to the implementing subclass.
     */
    public void testToQuery() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTQUERIES; runs++) {
            SearchExecutionContext context = createSearchExecutionContext();
            assert context.isCacheable();
            context.setAllowUnmappedFields(true);
            QB firstQuery = createTestQueryBuilder();
            QB controlQuery = copyQuery(firstQuery);
            /* we use a private rewrite context here since we want the most realistic way of asserting that we are cacheable or not.
             * We do it this way in SearchService where
             * we first rewrite the query with a private context, then reset the context and then build the actual lucene query*/
            QueryBuilder rewritten = rewriteQuery(firstQuery, createQueryRewriteContext(), new SearchExecutionContext(context));
            Query firstLuceneQuery = rewritten.toQuery(context);
            assertNotNull("toQuery should not return null", firstLuceneQuery);
            assertLuceneQuery(firstQuery, firstLuceneQuery, context);
            // remove after assertLuceneQuery since the assertLuceneQuery impl might access the context as well
            assertEquals(
                "query is not equal to its copy after calling toQuery, firstQuery: " + firstQuery + ", secondQuery: " + controlQuery,
                firstQuery,
                controlQuery
            );
            assertEquals(
                "equals is not symmetric after calling toQuery, firstQuery: " + firstQuery + ", secondQuery: " + controlQuery,
                controlQuery,
                firstQuery
            );
            assertThat(
                "query copy's hashcode is different from original hashcode after calling toQuery, firstQuery: "
                    + firstQuery
                    + ", secondQuery: "
                    + controlQuery,
                controlQuery.hashCode(),
                equalTo(firstQuery.hashCode())
            );

            QB secondQuery = copyQuery(firstQuery);
            // query _name never should affect the result of toQuery, we randomly set it to make sure
            if (randomBoolean()) {
                secondQuery.queryName(
                    secondQuery.queryName() == null
                        ? randomAlphaOfLengthBetween(1, 30)
                        : secondQuery.queryName() + randomAlphaOfLengthBetween(1, 10)
                );
            }
            context = new SearchExecutionContext(context);
            Query secondLuceneQuery = rewriteQuery(secondQuery, createQueryRewriteContext(), new SearchExecutionContext(context)).toQuery(
                context
            );
            assertNotNull("toQuery should not return null", secondLuceneQuery);
            assertLuceneQuery(secondQuery, secondLuceneQuery, context);

            if (builderGeneratesCacheableQueries()) {
                assertEquals(
                    "two equivalent query builders lead to different lucene queries hashcode",
                    secondLuceneQuery.hashCode(),
                    firstLuceneQuery.hashCode()
                );
                assertEquals(
                    "two equivalent query builders lead to different lucene queries",
                    rewrite(secondLuceneQuery),
                    rewrite(firstLuceneQuery)
                );
            }

            if (supportsBoost() && firstLuceneQuery instanceof MatchNoDocsQuery == false) {
                secondQuery.boost(firstQuery.boost() + 1f + randomFloat());
                Query thirdLuceneQuery = rewriteQuery(secondQuery, createQueryRewriteContext(), new SearchExecutionContext(context))
                    .toQuery(context);
                assertNotEquals(
                    "modifying the boost doesn't affect the corresponding lucene query",
                    rewrite(firstLuceneQuery),
                    rewrite(thirdLuceneQuery)
                );
            }
        }
    }

    /**
     * Simulate rewriting the query builder exclusively on the data node.
     * <br/>
     * <br/>
     * NOTE: This simulation does not reflect how the query builder will be rewritten in production.
     * See {@link AbstractQueryTestCase#rewriteQuery(AbstractQueryBuilder, QueryRewriteContext, SearchExecutionContext)} for a more accurate
     * simulation.
     *
     * @param queryBuilder The query builder to rewrite
     * @param shardRewriteContext The data node rewrite context
     * @return The rewritten query builder
     * @throws IOException
     */
    protected QueryBuilder rewriteQuery(QB queryBuilder, SearchExecutionContext shardRewriteContext) throws IOException {
        QueryBuilder rewritten = rewriteAndFetch(queryBuilder, shardRewriteContext);
        // extra safety to fail fast - serialize the rewritten version to ensure it's serializable.
        assertSerialization(rewritten);
        return rewritten;
    }

    /**
     * Simulate rewriting the query builder in stages across the coordinator node and data node.
     * It is rewritten on the coordinator node first, then again on the data node.
     *
     * @param queryBuilder The query builder to rewrite
     * @param coordinatorRewriteContext the coordinator node rewrite context
     * @param shardRewriteContext The data node rewrite context
     * @return The rewritten query builder
     * @throws IOException
     */
    protected QueryBuilder rewriteQuery(
        QB queryBuilder,
        QueryRewriteContext coordinatorRewriteContext,
        SearchExecutionContext shardRewriteContext
    ) throws IOException {
        // The first rewriteAndFetch call simulates rewriting on the coordinator node
        // The second rewriteAndFetch call simulates rewriting on the shard
        QueryBuilder rewritten = rewriteAndFetch(queryBuilder, coordinatorRewriteContext);
        // extra safety to fail fast - serialize the rewritten version to ensure it's serializable.
        assertSerialization(rewritten);
        rewritten = rewriteAndFetch(rewritten, shardRewriteContext);
        // extra safety to fail fast - serialize the rewritten version to ensure it's serializable.
        assertSerialization(rewritten);
        return rewritten;
    }

    /**
     * Few queries allow you to set the boost on the Java API, although the corresponding parser
     * doesn't parse it as it isn't supported. This method allows to disable boost related tests for those queries.
     * Those queries are easy to identify: their parsers don't parse {@code boost} as they don't apply to the specific query:
     * wrapper query and {@code match_none}.
     */
    protected boolean supportsBoost() {
        return true;
    }

    /**
     * Few queries allow you to set the query name on the Java API, although the corresponding parser
     * doesn't parse it as it isn't supported. This method allows to disable query name related tests for those queries.
     * Those queries are easy to identify: their parsers don't parse {@code _name} as they don't apply to the specific query:
     * wrapper query and {@code match_none}.
     */
    protected boolean supportsQueryName() {
        return true;
    }

    /**
     * Checks the result of {@link QueryBuilder#toQuery(SearchExecutionContext)} given the original {@link QueryBuilder}
     * and {@link SearchExecutionContext}. Verifies that named queries and boost are properly handled and delegates to
     * {@link #doAssertLuceneQuery(AbstractQueryBuilder, Query, SearchExecutionContext)} for query specific checks.
     */
    protected void assertLuceneQuery(QB queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        if (queryBuilder.queryName() != null && query instanceof MatchNoDocsQuery == false) {
            Query namedQuery = context.copyNamedQueries().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo(query));
        }
        if (query != null) {
            if (queryBuilder.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
                assertThat(query, either(instanceOf(BoostQuery.class)).or(instanceOf(MatchNoDocsQuery.class)));
                if (query instanceof BoostQuery boostQuery) {
                    if (boostQuery.getQuery() instanceof MatchNoDocsQuery == false) {
                        assertThat(boostQuery.getBoost(), equalTo(queryBuilder.boost()));
                    }
                    query = boostQuery.getQuery();
                }
            }
        }
        doAssertLuceneQuery(queryBuilder, query, context);
    }

    /**
     * Checks the result of {@link QueryBuilder#toQuery(SearchExecutionContext)} given the original {@link QueryBuilder}
     * and {@link SearchExecutionContext}. Contains the query specific checks to be implemented by subclasses.
     */
    protected abstract void doAssertLuceneQuery(QB queryBuilder, Query query, SearchExecutionContext context) throws IOException;

    protected void assertTermOrBoostQuery(Query query, String field, String value, float fieldBoost) {
        if (fieldBoost != AbstractQueryBuilder.DEFAULT_BOOST) {
            assertThat(query, instanceOf(BoostQuery.class));
            BoostQuery boostQuery = (BoostQuery) query;
            assertThat(boostQuery.getBoost(), equalTo(fieldBoost));
            query = boostQuery.getQuery();
        }
        assertTermQuery(query, field, value);
    }

    protected void assertTermQuery(Query query, String field, String value) {
        assertThat(query, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) query;

        String expectedFieldName = expectedFieldName(field);
        assertThat(termQuery.getTerm().field(), equalTo(expectedFieldName));
        assertThat(termQuery.getTerm().text().toLowerCase(Locale.ROOT), equalTo(value.toLowerCase(Locale.ROOT)));
    }

    /**
     * Test serialization and deserialization of the test query.
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTQUERIES; runs++) {
            QB testQuery = createTestQueryBuilder();
            assertSerialization(testQuery);
        }
    }

    protected QueryBuilder assertSerialization(QueryBuilder testQuery) throws IOException {
        return assertSerialization(testQuery, TransportVersion.current());
    }

    /**
     * Serialize the given query builder and asserts that both are equal
     */
    protected QueryBuilder assertSerialization(QueryBuilder testQuery, TransportVersion version) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(version);
            output.writeNamedWriteable(testQuery);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry())) {
                in.setTransportVersion(version);
                QueryBuilder deserializedQuery = in.readNamedWriteable(QueryBuilder.class);
                assertEquals(testQuery, deserializedQuery);
                assertEquals(testQuery.hashCode(), deserializedQuery.hashCode());
                assertNotSame(testQuery, deserializedQuery);
                return deserializedQuery;
            }
        }
    }

    public void testEqualsAndHashcode() {
        for (int runs = 0; runs < NUMBER_OF_TESTQUERIES; runs++) {
            // TODO we only change name and boost, we should extend by any sub-test supplying a "mutate" method that randomly changes one
            // aspect of the object under test
            checkEqualsAndHashCode(createTestQueryBuilder(), this::copyQuery, this::mutateInstance);
        }
    }

    public QB mutateInstance(QB instance) throws IOException {
        return changeNameOrBoost(instance);
    }

    /**
     * Generic test that checks that the <code>Strings.toString()</code> method
     * renders the XContent correctly.
     */
    public void testValidOutput() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTQUERIES; runs++) {
            QB testQuery = createTestQueryBuilder();
            XContentType xContentType = XContentType.JSON;
            String toString = Strings.toString(testQuery);
            try (var parser = createParser(xContentType.xContent(), toString)) {
                assertParsedQuery(parser, testQuery);
            }
            BytesReference bytes = XContentHelper.toXContent(testQuery, xContentType, false);
            try (var parser = createParser(xContentType.xContent(), bytes)) {
                assertParsedQuery(parser, testQuery);
            }
        }
    }

    protected QB changeNameOrBoost(QB original) throws IOException {
        QB secondQuery = copyQuery(original);
        if (randomBoolean()) {
            secondQuery.queryName(
                secondQuery.queryName() == null
                    ? randomAlphaOfLengthBetween(1, 30)
                    : secondQuery.queryName() + randomAlphaOfLengthBetween(1, 10)
            );
        } else {
            secondQuery.boost(original.boost() + 1f + randomFloat());
        }
        return secondQuery;
    }

    // we use the streaming infra to create a copy of the query provided as argument
    @SuppressWarnings("unchecked")
    protected QB copyQuery(QB query) throws IOException {
        Reader<QB> reader = (Reader<QB>) namedWriteableRegistry().getReader(QueryBuilder.class, query.getWriteableName());
        return copyWriteable(query, namedWriteableRegistry(), reader);
    }

    /**
     * create a random value for either {@link AbstractQueryTestCase#BOOLEAN_FIELD_NAME}, {@link AbstractQueryTestCase#INT_FIELD_NAME},
     * {@link AbstractQueryTestCase#DOUBLE_FIELD_NAME}, {@link AbstractQueryTestCase#TEXT_FIELD_NAME} or
     * {@link AbstractQueryTestCase#DATE_FIELD_NAME} or {@link AbstractQueryTestCase#DATE_NANOS_FIELD_NAME} or a String value by default
     */
    protected static Object getRandomValueForFieldName(String fieldName) {
        Object value;
        switch (fieldName) {
            case TEXT_FIELD_NAME:
            case TEXT_ALIAS_FIELD_NAME:
                if (rarely()) {
                    // unicode in 10% cases
                    JsonStringEncoder encoder = JsonStringEncoder.getInstance();
                    value = new String(encoder.quoteAsString(randomUnicodeOfLength(10)));
                } else {
                    value = randomAlphaOfLengthBetween(1, 10);
                }
                break;
            case INT_FIELD_NAME:
            case INT_ALIAS_FIELD_NAME:
                value = randomIntBetween(0, 10);
                break;
            case DOUBLE_FIELD_NAME:
                value = 1 + randomDouble() * 9;
                break;
            case BOOLEAN_FIELD_NAME:
                value = randomBoolean();
                break;
            case DATE_FIELD_NAME:
                value = ZonedDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneOffset.UTC).toString();
                break;
            case DATE_NANOS_FIELD_NAME:
                value = Instant.now().toString();
                break;
            default:
                value = randomAlphaOfLengthBetween(1, 10);
        }
        return value;
    }

    protected static String getRandomQueryText() {
        int terms = randomIntBetween(0, 3);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < terms; i++) {
            builder.append(randomAlphaOfLengthBetween(1, 10)).append(" ");
        }
        return builder.toString().trim();
    }

    /**
     * Helper method to return a mapped or a random field
     */
    protected static String getRandomFieldName() {
        // if no type is set then return a random field name
        if (randomBoolean()) {
            return randomAlphaOfLengthBetween(1, 10);
        } else {
            return randomFrom(MAPPED_LEAF_FIELD_NAMES);
        }
    }

    /**
     * Helper method to return a random rewrite method
     */
    protected static String getRandomRewriteMethod() {
        String rewrite;
        if (randomBoolean()) {
            rewrite = randomFrom(
                QueryParsers.CONSTANT_SCORE,
                QueryParsers.SCORING_BOOLEAN,
                QueryParsers.CONSTANT_SCORE_BOOLEAN,
                QueryParsers.CONSTANT_SCORE_BLENDED
            ).getPreferredName();
        } else {
            rewrite = randomFrom(QueryParsers.TOP_TERMS, QueryParsers.TOP_TERMS_BOOST, QueryParsers.TOP_TERMS_BLENDED_FREQS)
                .getPreferredName()
                + "1";
        }
        return rewrite;
    }

    protected static Fuzziness randomFuzziness(String fieldName) {
        switch (fieldName) {
            case INT_FIELD_NAME:
            case INT_ALIAS_FIELD_NAME:
            case DOUBLE_FIELD_NAME:
            case DATE_FIELD_NAME:
            case DATE_NANOS_FIELD_NAME:
                return Fuzziness.fromEdits(randomIntBetween(0, 2));
            default:
                if (randomBoolean()) {
                    return Fuzziness.fromEdits(randomIntBetween(0, 2));
                }
                return Fuzziness.AUTO;
        }
    }

    protected static String randomAnalyzer() {
        return randomFrom("simple", "standard", "keyword", "whitespace");
    }

    protected static String randomMinimumShouldMatch() {
        return randomFrom("1", "-1", "75%", "-25%", "2<75%", "2<-25%");
    }

    /**
     * Call this method to check a valid json string representing the query under test against
     * it's generated json.
     *
     * Note: By the time of this writing (Nov 2015) all queries are taken from the query dsl
     * reference docs mirroring examples there. Here's how the queries were generated:
     *
     * <ul>
     * <li> Take a reference documentation example.
     * <li> Stick it into the createParseableQueryJson method of the respective query test.
     * <li> Manually check that what the QueryBuilder generates equals the input json ignoring default options.
     * <li> Put the manual checks into the assertQueryParsedFromJson method.
     * <li> Now copy the generated json including default options into createParseableQueryJson
     * <li> By now the roundtrip check for the json should be happy.
     * </ul>
     **/
    public static void checkGeneratedJson(String expected, QueryBuilder source) throws IOException {
        // now assert that we actually generate the same JSON
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        source.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            msg(expected, Strings.toString(builder)),
            expected.replaceAll("\\s+", ""),
            Strings.toString(builder).replaceAll("\\s+", "")
        );
    }

    private static String msg(String left, String right) {
        int size = Math.min(left.length(), right.length());
        StringBuilder builder = new StringBuilder("size: " + left.length() + " vs. " + right.length());
        builder.append(" content: <<");
        for (int i = 0; i < size; i++) {
            if (left.charAt(i) == right.charAt(i)) {
                builder.append(left.charAt(i));
            } else {
                builder.append(">> ")
                    .append("until offset: ")
                    .append(i)
                    .append(" [")
                    .append(left.charAt(i))
                    .append(" vs.")
                    .append(right.charAt(i))
                    .append("] [")
                    .append((int) left.charAt(i))
                    .append(" vs.")
                    .append((int) right.charAt(i))
                    .append(']');
                return builder.toString();
            }
        }
        if (left.length() != right.length()) {
            int leftEnd = Math.max(size, left.length()) - 1;
            int rightEnd = Math.max(size, right.length()) - 1;
            builder.append(">> ")
                .append("until offset: ")
                .append(size)
                .append(" [")
                .append(left.charAt(leftEnd))
                .append(" vs.")
                .append(right.charAt(rightEnd))
                .append("] [")
                .append((int) left.charAt(leftEnd))
                .append(" vs.")
                .append((int) right.charAt(rightEnd))
                .append(']');
            return builder.toString();
        }
        return "";
    }

    /**
     * This test ensures that queries that need to be rewritten have dedicated tests.
     * These queries must override this method accordingly.
     */
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        QB queryBuilder = createTestQueryBuilder();
        queryBuilder.toQuery(context);
    }

    protected Query rewrite(Query query) throws IOException {
        return query;
    }

    protected QueryBuilder rewriteAndFetch(QueryBuilder builder, QueryRewriteContext context) {
        PlainActionFuture<QueryBuilder> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(builder, context, future);
        return future.actionGet();
    }

    public boolean isTextField(String fieldName) {
        return fieldName.equals(TEXT_FIELD_NAME) || fieldName.equals(TEXT_ALIAS_FIELD_NAME);
    }

    /**
     * Check that a query is generally cacheable. Tests for query builders that are not always cacheable
     * should overwrite this method and make sure the different cases are always tested
     */
    public void testCacheability() throws IOException {
        QB queryBuilder = createTestQueryBuilder();
        SearchExecutionContext context = createSearchExecutionContext();
        QueryBuilder rewriteQuery = rewriteQuery(queryBuilder, createQueryRewriteContext(), new SearchExecutionContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertTrue("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }
}
