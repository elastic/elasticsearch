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

package org.elasticsearch.test;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;


public abstract class AbstractQueryTestCase<QB extends AbstractQueryBuilder<QB>> extends ESTestCase {

    public static final String STRING_FIELD_NAME = "mapped_string";
    protected static final String STRING_FIELD_NAME_2 = "mapped_string_2";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_FIELD_NAME = "mapped_date";
    protected static final String OBJECT_FIELD_NAME = "mapped_object";
    protected static final String GEO_POINT_FIELD_NAME = "mapped_geo_point";
    protected static final String GEO_SHAPE_FIELD_NAME = "mapped_geo_shape";
    protected static final String[] MAPPED_FIELD_NAMES = new String[]{STRING_FIELD_NAME, INT_FIELD_NAME,
            DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, DATE_FIELD_NAME, OBJECT_FIELD_NAME, GEO_POINT_FIELD_NAME,
            GEO_SHAPE_FIELD_NAME};
    private static final String[] MAPPED_LEAF_FIELD_NAMES = new String[]{STRING_FIELD_NAME, INT_FIELD_NAME,
            DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, DATE_FIELD_NAME, GEO_POINT_FIELD_NAME, };
    private static final int NUMBER_OF_TESTQUERIES = 20;

    protected static Version indexVersionCreated;

    private static ServiceHolder serviceHolder;
    private static int queryNameId = 0;
    private static Settings nodeSettings;
    private static Index index;
    private static String[] currentTypes;
    private static String[] randomTypes;

    protected static Index getIndex() {
        return index;
    }

    protected static String[] getCurrentTypes() {
        return currentTypes;
    }

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.emptyList();
    }

    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
    }

    @BeforeClass
    public static void beforeClass() {
        nodeSettings = Settings.builder()
                .put("node.name", AbstractQueryTestCase.class.toString())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .build();

        index = new Index(randomAlphaOfLengthBetween(1, 10), "_na_");

        // Set a single type in the index
        switch (random().nextInt(3)) {
        case 0:
            currentTypes = new String[0]; // no types
            break;
        default:
            currentTypes = new String[] { "doc" };
            break;
        }
        randomTypes = getRandomTypes();
    }

    protected Settings indexSettings() {
        // we have to prefer CURRENT since with the range of versions we support it's rather unlikely to get the current actually.
        indexVersionCreated = randomBoolean() ? Version.CURRENT
                : VersionUtils.randomVersionBetween(random(), null, Version.CURRENT);
        return Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, indexVersionCreated)
            .build();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        IOUtils.close(serviceHolder);
        serviceHolder = null;
    }

    @Before
    public void beforeTest() throws IOException {
        if (serviceHolder == null) {
            serviceHolder = new ServiceHolder(nodeSettings, indexSettings(), getPlugins(), this);
        }
        serviceHolder.clientInvocationHandler.delegate = this;
    }

    private static SearchContext getSearchContext(String[] types, QueryShardContext context) {
        TestSearchContext testSearchContext = new TestSearchContext(context) {
            @Override
            public MapperService mapperService() {
                return serviceHolder.mapperService; // need to build / parse inner hits sort fields
            }

            @Override
            public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType) {
                return serviceHolder.indexFieldDataService.getForField(fieldType); // need to build / parse inner hits sort fields
            }

        };
        testSearchContext.getQueryShardContext().setTypes(types);
        return testSearchContext;
    }

    @After
    public void afterTest() {
        serviceHolder.clientInvocationHandler.delegate = null;
    }

    public final QB createTestQueryBuilder() {
        QB query = doCreateTestQueryBuilder();
        //we should not set boost and query name for queries that don't parse it
        if (supportsBoostAndQueryName()) {
            if (randomBoolean()) {
                query.boost(2.0f / randomIntBetween(1, 20));
            }
            if (randomBoolean()) {
                query.queryName(createUniqueRandomName());
            }
        }
        return query;
    }

    /**
     * make sure query names are unique by suffixing them with increasing counter
     */
    private static String createUniqueRandomName() {
        String queryName = randomAlphaOfLengthBetween(1, 10) + queryNameId;
        queryNameId++;
        return queryName;
    }

    /**
     * Create the query that is being tested
     */
    protected abstract QB doCreateTestQueryBuilder();

    /**
     * Generic test that creates new query from the test query and checks both for equality
     * and asserts equality on the two queries.
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTQUERIES; runs++) {
            QB testQuery = createTestQueryBuilder();
            XContentType xContentType = randomFrom(XContentType.values());
            BytesReference shuffledXContent = toShuffledXContent(testQuery, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean(),
                    shuffleProtectedFields());
            assertParsedQuery(createParser(xContentType.xContent(), shuffledXContent), testQuery);
            for (Map.Entry<String, QB> alternateVersion : getAlternateVersions().entrySet()) {
                String queryAsString = alternateVersion.getKey();
                assertParsedQuery(createParser(JsonXContent.jsonXContent, queryAsString), alternateVersion.getValue());
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
    public void testUnknownField() {
        String marker = "#marker#";
        QB testQuery;
        do {
            testQuery = createTestQueryBuilder();
        } while (testQuery.toString().contains(marker));
        testQuery.queryName(marker); // to find root query to add additional bogus field there
        String queryAsString = testQuery.toString().replace("\"" + marker + "\"", "\"" + marker + "\", \"bogusField\" : \"someValue\"");
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(queryAsString));
        // we'd like to see the offending field name here
        assertThat(e.getMessage(), containsString("bogusField"));
    }

    /**
     * Test that adding an additional object within each object of the otherwise correct query always triggers some kind of
     * parse exception. Some specific objects do not cause any exception as they can hold arbitrary content; they can be
     * declared by overriding {@link #getObjectsHoldingArbitraryContent()}.
     */
    public final void testUnknownObjectException() throws IOException {
        Set<String> candidates = new HashSet<>();
        // Adds the valid query to the list of queries to modify and test
        candidates.add(createTestQueryBuilder().toString());
        // Adds the alternates versions of the query too
        candidates.addAll(getAlternateVersions().keySet());

        List<Tuple<String, Boolean>> testQueries = alterateQueries(candidates, getObjectsHoldingArbitraryContent());
        for (Tuple<String, Boolean> testQuery : testQueries) {
            boolean expectedException = testQuery.v2();
            try {
                parseQuery(testQuery.v1());
                if (expectedException) {
                    fail("some parsing exception expected for query: " + testQuery);
                }
            } catch (ParsingException | ElasticsearchParseException e) {
                // different kinds of exception wordings depending on location
                // of mutation, so no simple asserts possible here
                if (expectedException == false) {
                    throw new AssertionError("unexpected exception when parsing query:\n" + testQuery, e);
                }
            } catch (IllegalArgumentException e) {
                if (expectedException == false) {
                    throw new AssertionError("unexpected exception when parsing query:\n" + testQuery, e);
                }
                assertThat(e.getMessage(), containsString("unknown field [newField], parser not found"));
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
    static List<Tuple<String, Boolean>> alterateQueries(Set<String> queries, Set<String> arbitraryMarkers) throws IOException {
        List<Tuple<String, Boolean>> results = new ArrayList<>();

        // Indicate if a part of the query can hold any arbitrary content
        boolean hasArbitraryContent = (arbitraryMarkers != null && arbitraryMarkers.isEmpty() == false);

        for (String query : queries) {
            // Track the number of query mutations
            int mutation = 0;

            while (true) {
                boolean expectException = true;

                BytesStreamOutput out = new BytesStreamOutput();
                try (
                        XContentGenerator generator = XContentType.JSON.xContent().createGenerator(out);
                        XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, query);
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
                                XContentHelper.copyCurrentStructure(generator, parser);
                                generator.writeEndObject();

                                if (hasArbitraryContent) {
                                    // The query has one or more fields that hold arbitrary content. If the current
                                    // field is one (or a child) of those, no exception is expected when parsing the mutated query.
                                    for (String marker : arbitraryMarkers) {
                                        if (levels.contains(marker)) {
                                            expectException = false;
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
                        XContentHelper.copyCurrentEvent(generator, parser);
                    }

                    if (objectIndex < mutation) {
                        // We did not reach the insertion point, there's no more mutations to try
                        break;
                    } else {
                        // We reached the expected insertion point, so next time we'll try one step further
                        mutation++;
                    }
                }

                results.add(new Tuple<>(out.bytes().utf8ToString(), expectException));
            }
        }
        return results;
    }

    /**
     * Returns a set of object names that won't trigger any exception (uncluding their children) when testing that unknown
     * objects cause parse exceptions through {@link #testUnknownObjectException()}. Default is an empty set. Can be overridden
     * by subclasses that test queries which contain objects that get parsed on the data nodes (e.g. score functions) or objects
     * that can contain arbitrary content (e.g. documents for percolate or more like this query, params for scripts). In such
     * cases no exception would get thrown.
     */
    protected Set<String> getObjectsHoldingArbitraryContent() {
        return Collections.emptySet();
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

        String testQuery = validQuery.substring(0, insertionPosition) + "[" +
                validQuery.substring(insertionPosition, endArrayPosition) + "]" +
                validQuery.substring(endArrayPosition, validQuery.length());

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
    private static void assertParsedQuery(XContentParser parser, QueryBuilder expectedQuery) throws IOException {
        QueryBuilder newQuery = parseQuery(parser);
        assertNotSame(newQuery, expectedQuery);
        assertEquals(expectedQuery, newQuery);
        assertEquals(expectedQuery.hashCode(), newQuery.hashCode());
    }

    protected QueryBuilder parseQuery(AbstractQueryBuilder<?> builder) throws IOException {
        BytesReference bytes = XContentHelper.toXContent(builder, XContentType.JSON, false);
        return parseQuery(createParser(JsonXContent.jsonXContent, bytes));
    }

    protected QueryBuilder parseQuery(String queryAsString) throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, queryAsString);
        return parseQuery(parser);
    }

    protected static QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder parseInnerQueryBuilder = parseInnerQueryBuilder(parser);
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
            QueryShardContext context = createShardContext();
            assert context.isCachable();
            context.setAllowUnmappedFields(true);
            QB firstQuery = createTestQueryBuilder();
            QB controlQuery = copyQuery(firstQuery);
            SearchContext searchContext = getSearchContext(randomTypes, context);
            /* we use a private rewrite context here since we want the most realistic way of asserting that we are cacheable or not.
             * We do it this way in SearchService where
             * we first rewrite the query with a private context, then reset the context and then build the actual lucene query*/
            QueryBuilder rewritten = rewriteQuery(firstQuery, new QueryShardContext(context));
            Query firstLuceneQuery = rewritten.toQuery(context);
            if (isCachable(firstQuery)) {
                assertTrue("query was marked as not cacheable in the context but this test indicates it should be cacheable: "
                        + firstQuery.toString(), context.isCachable());
            } else {
                assertFalse("query was marked as cacheable in the context but this test indicates it should not be cacheable: "
                        + firstQuery.toString(), context.isCachable());
            }
            assertNotNull("toQuery should not return null", firstLuceneQuery);
            assertLuceneQuery(firstQuery, firstLuceneQuery, searchContext);
            //remove after assertLuceneQuery since the assertLuceneQuery impl might access the context as well
            assertTrue(
                    "query is not equal to its copy after calling toQuery, firstQuery: " + firstQuery + ", secondQuery: " + controlQuery,
                    firstQuery.equals(controlQuery));
            assertTrue("equals is not symmetric after calling toQuery, firstQuery: " + firstQuery + ", secondQuery: " + controlQuery,
                    controlQuery.equals(firstQuery));
            assertThat("query copy's hashcode is different from original hashcode after calling toQuery, firstQuery: " + firstQuery
                    + ", secondQuery: " + controlQuery, controlQuery.hashCode(), equalTo(firstQuery.hashCode()));

            QB secondQuery = copyQuery(firstQuery);
            // query _name never should affect the result of toQuery, we randomly set it to make sure
            if (randomBoolean()) {
                secondQuery.queryName(secondQuery.queryName() == null ? randomAlphaOfLengthBetween(1, 30) : secondQuery.queryName()
                        + randomAlphaOfLengthBetween(1, 10));
            }
            searchContext = getSearchContext(randomTypes, context);
            Query secondLuceneQuery = rewriteQuery(secondQuery, context).toQuery(context);
            assertNotNull("toQuery should not return null", secondLuceneQuery);
            assertLuceneQuery(secondQuery, secondLuceneQuery, searchContext);

            if (builderGeneratesCacheableQueries()) {
                assertEquals("two equivalent query builders lead to different lucene queries",
                        rewrite(secondLuceneQuery), rewrite(firstLuceneQuery));
            }

            if (supportsBoostAndQueryName()) {
                secondQuery.boost(firstQuery.boost() + 1f + randomFloat());
                Query thirdLuceneQuery = rewriteQuery(secondQuery, context).toQuery(context);
                assertNotEquals("modifying the boost doesn't affect the corresponding lucene query", rewrite(firstLuceneQuery),
                        rewrite(thirdLuceneQuery));
            }

            // check that context#isFilter is not changed by invoking toQuery/rewrite
            boolean filterFlag = randomBoolean();
            context.setIsFilter(filterFlag);
            rewriteQuery(firstQuery, context).toQuery(context);
            assertEquals("isFilter should be unchanged", filterFlag, context.isFilter());
        }
    }

    private QueryBuilder rewriteQuery(QB queryBuilder, QueryRewriteContext rewriteContext) throws IOException {
        QueryBuilder rewritten = rewriteAndFetch(queryBuilder, rewriteContext);
        // extra safety to fail fast - serialize the rewritten version to ensure it's serializable.
        assertSerialization(rewritten);
        return rewritten;
    }

    protected boolean isCachable(QB queryBuilder) {
        return true;
    }

    /**
     * Few queries allow you to set the boost and queryName on the java api, although the corresponding parser
     * doesn't parse them as they are not supported. This method allows to disable boost and queryName related tests for those queries.
     * Those queries are easy to identify: their parsers don't parse `boost` and `_name` as they don't apply to the specific query:
     * wrapper query and match_none
     */
    protected boolean supportsBoostAndQueryName() {
        return true;
    }

    /**
     * Checks the result of {@link QueryBuilder#toQuery(QueryShardContext)} given the original {@link QueryBuilder}
     * and {@link QueryShardContext}. Verifies that named queries and boost are properly handled and delegates to
     * {@link #doAssertLuceneQuery(AbstractQueryBuilder, Query, SearchContext)} for query specific checks.
     */
    private void assertLuceneQuery(QB queryBuilder, Query query, SearchContext context) throws IOException {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.getQueryShardContext().copyNamedQueries().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo(query));
        }
        if (query != null) {
            if (queryBuilder.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
                assertThat(query, either(instanceOf(BoostQuery.class)).or(instanceOf(SpanBoostQuery.class)));
                if (query instanceof SpanBoostQuery) {
                    SpanBoostQuery spanBoostQuery = (SpanBoostQuery) query;
                    assertThat(spanBoostQuery.getBoost(), equalTo(queryBuilder.boost()));
                    query = spanBoostQuery.getQuery();
                } else {
                    BoostQuery boostQuery = (BoostQuery) query;
                    assertThat(boostQuery.getBoost(), equalTo(queryBuilder.boost()));
                    query = boostQuery.getQuery();
                }
            }
        }
        doAssertLuceneQuery(queryBuilder, query, context);
    }

    /**
     * Checks the result of {@link QueryBuilder#toQuery(QueryShardContext)} given the original {@link QueryBuilder}
     * and {@link QueryShardContext}. Contains the query specific checks to be implemented by subclasses.
     */
    protected abstract void doAssertLuceneQuery(QB queryBuilder, Query query, SearchContext context) throws IOException;

    protected static void assertTermOrBoostQuery(Query query, String field, String value, float fieldBoost) {
        if (fieldBoost != AbstractQueryBuilder.DEFAULT_BOOST) {
            assertThat(query, instanceOf(BoostQuery.class));
            BoostQuery boostQuery = (BoostQuery) query;
            assertThat(boostQuery.getBoost(), equalTo(fieldBoost));
            query = boostQuery.getQuery();
        }
        assertTermQuery(query, field, value);
    }

    protected static void assertTermQuery(Query query, String field, String value) {
        assertThat(query, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) query;
        assertThat(termQuery.getTerm().field(), equalTo(field));
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

    protected static QueryBuilder assertSerialization(QueryBuilder testQuery) throws IOException {
        return assertSerialization(testQuery, Version.CURRENT);
    }

    /**
     * Serialize the given query builder and asserts that both are equal
     */
    protected static QueryBuilder assertSerialization(QueryBuilder testQuery, Version version) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(version);
            output.writeNamedWriteable(testQuery);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), serviceHolder.namedWriteableRegistry)) {
                in.setVersion(version);
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
            checkEqualsAndHashCode(createTestQueryBuilder(), this::copyQuery, this::changeNameOrBoost);
        }
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
            assertParsedQuery(createParser(xContentType.xContent(), toString), testQuery);
            BytesReference bytes = XContentHelper.toXContent(testQuery, xContentType, false);
            assertParsedQuery(createParser(xContentType.xContent(), bytes), testQuery);
        }
    }

    private QB changeNameOrBoost(QB original) throws IOException {
        QB secondQuery = copyQuery(original);
        if (randomBoolean()) {
            secondQuery.queryName(secondQuery.queryName() == null ? randomAlphaOfLengthBetween(1, 30) : secondQuery.queryName()
                    + randomAlphaOfLengthBetween(1, 10));
        } else {
            secondQuery.boost(original.boost() + 1f + randomFloat());
        }
        return secondQuery;
    }

    //we use the streaming infra to create a copy of the query provided as argument
    @SuppressWarnings("unchecked")
    private QB copyQuery(QB query) throws IOException {
        Reader<QB> reader = (Reader<QB>) serviceHolder.namedWriteableRegistry.getReader(QueryBuilder.class, query.getWriteableName());
        return copyWriteable(query, serviceHolder.namedWriteableRegistry, reader);
    }

    /**
     * @return a new {@link QueryShardContext} based on the base test index and queryParserService
     */
    protected static QueryShardContext createShardContext() {
        return serviceHolder.createShardContext();
    }

    /**
     * create a random value for either {@link AbstractQueryTestCase#BOOLEAN_FIELD_NAME}, {@link AbstractQueryTestCase#INT_FIELD_NAME},
     * {@link AbstractQueryTestCase#DOUBLE_FIELD_NAME}, {@link AbstractQueryTestCase#STRING_FIELD_NAME} or
     * {@link AbstractQueryTestCase#DATE_FIELD_NAME}, or a String value by default
     */
    protected static Object getRandomValueForFieldName(String fieldName) {
        Object value;
        switch (fieldName) {
            case STRING_FIELD_NAME:
                if (rarely()) {
                    // unicode in 10% cases
                    JsonStringEncoder encoder = JsonStringEncoder.getInstance();
                    value = new String(encoder.quoteAsString(randomUnicodeOfLength(10)));
                } else {
                    value = randomAlphaOfLengthBetween(1, 10);
                }
                break;
            case INT_FIELD_NAME:
                value = randomIntBetween(0, 10);
                break;
            case DOUBLE_FIELD_NAME:
                value = 1 + randomDouble() * 9;
                break;
            case BOOLEAN_FIELD_NAME:
                value = randomBoolean();
                break;
            case DATE_FIELD_NAME:
                value = new DateTime(System.currentTimeMillis(), DateTimeZone.UTC).toString();
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
        if (currentTypes.length == 0 || randomBoolean()) {
            return randomAlphaOfLengthBetween(1, 10);
        }
        return randomFrom(MAPPED_LEAF_FIELD_NAMES);
    }

    /**
     * Helper method to return a random rewrite method
     */
    protected static String getRandomRewriteMethod() {
        String rewrite;
        if (randomBoolean()) {
            rewrite = randomFrom(QueryParsers.CONSTANT_SCORE,
                    QueryParsers.SCORING_BOOLEAN,
                    QueryParsers.CONSTANT_SCORE_BOOLEAN).getPreferredName();
        } else {
            rewrite = randomFrom(QueryParsers.TOP_TERMS,
                    QueryParsers.TOP_TERMS_BOOST,
                    QueryParsers.TOP_TERMS_BLENDED_FREQS).getPreferredName() + "1";
        }
        return rewrite;
    }

    private static String[] getRandomTypes() {
        String[] types;
        if (currentTypes.length > 0 && randomBoolean()) {
            int numberOfQueryTypes = randomIntBetween(1, currentTypes.length);
            types = new String[numberOfQueryTypes];
            for (int i = 0; i < numberOfQueryTypes; i++) {
                types[i] = randomFrom(currentTypes);
            }
        } else {
            if (randomBoolean()) {
                types = new String[]{MetaData.ALL};
            } else {
                types = new String[0];
            }
        }
        return types;
    }

    protected static Fuzziness randomFuzziness(String fieldName) {
        switch (fieldName) {
            case INT_FIELD_NAME:
                return Fuzziness.build(randomIntBetween(3, 100));
            case DOUBLE_FIELD_NAME:
                return Fuzziness.build(1 + randomFloat() * 10);
            case DATE_FIELD_NAME:
                return Fuzziness.build(randomTimeValue());
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

    private static class ClientInvocationHandler implements InvocationHandler {
        AbstractQueryTestCase<?> delegate;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.equals(Client.class.getMethod("get", GetRequest.class, ActionListener.class))){
                GetResponse getResponse = delegate.executeGet((GetRequest) args[0]);
                ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
                if (randomBoolean()) {
                    listener.onResponse(getResponse);
                } else {
                    new Thread(() -> listener.onResponse(getResponse)).start();
                }
                return null;
            } else if (method.equals(Client.class.getMethod
                ("multiTermVectors", MultiTermVectorsRequest.class))) {
                return new PlainActionFuture<MultiTermVectorsResponse>() {
                    @Override
                    public MultiTermVectorsResponse get() throws InterruptedException, ExecutionException {
                        return delegate.executeMultiTermVectors((MultiTermVectorsRequest) args[0]);
                    }
                };
            } else if (method.equals(Object.class.getMethod("toString"))) {
                return "MockClient";
            }
            throw new UnsupportedOperationException("this test can't handle calls to: " + method);
        }

    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers / builders
     */
    protected GetResponse executeGet(GetRequest getRequest) {
        throw new UnsupportedOperationException("this test can't handle GET requests");
    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers / builders
     */
    protected MultiTermVectorsResponse executeMultiTermVectors(MultiTermVectorsRequest mtvRequest) {
        throw new UnsupportedOperationException("this test can't handle MultiTermVector requests");
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
                msg(expected, builder.string()),
                expected.replaceAll("\\s+", ""),
                builder.string().replaceAll("\\s+", ""));
    }

    private static String msg(String left, String right) {
        int size = Math.min(left.length(), right.length());
        StringBuilder builder = new StringBuilder("size: " + left.length() + " vs. " + right.length());
        builder.append(" content: <<");
        for (int i = 0; i < size; i++) {
            if (left.charAt(i) == right.charAt(i)) {
                builder.append(left.charAt(i));
            } else {
                builder.append(">> ").append("until offset: ").append(i)
                        .append(" [").append(left.charAt(i)).append(" vs.").append(right.charAt(i))
                        .append("] [").append((int) left.charAt(i)).append(" vs.").append((int) right.charAt(i)).append(']');
                return builder.toString();
            }
        }
        if (left.length() != right.length()) {
            int leftEnd = Math.max(size, left.length()) - 1;
            int rightEnd = Math.max(size, right.length()) - 1;
            builder.append(">> ").append("until offset: ").append(size)
                    .append(" [").append(left.charAt(leftEnd)).append(" vs.").append(right.charAt(rightEnd))
                    .append("] [").append((int) left.charAt(leftEnd)).append(" vs.").append((int) right.charAt(rightEnd)).append(']');
            return builder.toString();
        }
        return "";
    }

    /**
     * This test ensures that queries that need to be rewritten have dedicated tests.
     * These queries must override this method accordingly.
     */
    public void testMustRewrite() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);
        QB queryBuilder = createTestQueryBuilder();
        queryBuilder.toQuery(context);
    }

    protected Query rewrite(Query query) throws IOException {
        return query;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return serviceHolder.xContentRegistry;
    }

    private static class ServiceHolder implements Closeable {
        private final IndexFieldDataService indexFieldDataService;
        private final SearchModule searchModule;
        private final NamedWriteableRegistry namedWriteableRegistry;
        private final NamedXContentRegistry xContentRegistry;
        private final ClientInvocationHandler clientInvocationHandler = new ClientInvocationHandler();
        private final IndexSettings idxSettings;
        private final SimilarityService similarityService;
        private final MapperService mapperService;
        private final BitsetFilterCache bitsetFilterCache;
        private final ScriptService scriptService;
        private final Client client;
        private final long nowInMillis = randomNonNegativeLong();

        ServiceHolder(Settings nodeSettings, Settings indexSettings,
                      Collection<Class<? extends Plugin>> plugins, AbstractQueryTestCase<?> testCase) throws IOException {
            Environment env = InternalSettingsPreparer.prepareEnvironment(nodeSettings, null);
            PluginsService pluginsService;
            pluginsService = new PluginsService(nodeSettings, null, env.modulesFile(), env.pluginsFile(), plugins);

            client = (Client) Proxy.newProxyInstance(
                    Client.class.getClassLoader(),
                    new Class[]{Client.class},
                    clientInvocationHandler);
            ScriptModule scriptModule = createScriptModule(pluginsService.filterPlugins(ScriptPlugin.class));
            List<Setting<?>> additionalSettings = pluginsService.getPluginSettings();
            additionalSettings.add(InternalSettingsPlugin.VERSION_CREATED);
            SettingsModule settingsModule = new SettingsModule(nodeSettings, additionalSettings, pluginsService.getPluginSettingsFilter());
            searchModule = new SearchModule(nodeSettings, false, pluginsService.filterPlugins(SearchPlugin.class));
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
            entries.addAll(indicesModule.getNamedWriteables());
            entries.addAll(searchModule.getNamedWriteables());
            namedWriteableRegistry = new NamedWriteableRegistry(entries);
            xContentRegistry = new NamedXContentRegistry(Stream.of(
                    searchModule.getNamedXContents().stream()
                    ).flatMap(Function.identity()).collect(toList()));
            IndexScopedSettings indexScopedSettings = settingsModule.getIndexScopedSettings();
            idxSettings = IndexSettingsModule.newIndexSettings(index, indexSettings, indexScopedSettings);
            AnalysisModule analysisModule = new AnalysisModule(TestEnvironment.newEnvironment(nodeSettings), emptyList());
            IndexAnalyzers indexAnalyzers = analysisModule.getAnalysisRegistry().build(idxSettings);
            scriptService = scriptModule.getScriptService();
            similarityService = new SimilarityService(idxSettings, null, Collections.emptyMap());
            MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
            mapperService = new MapperService(idxSettings, indexAnalyzers, xContentRegistry, similarityService, mapperRegistry,
                    this::createShardContext);
            IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(nodeSettings, new IndexFieldDataCache.Listener() {
            });
            indexFieldDataService = new IndexFieldDataService(idxSettings, indicesFieldDataCache,
                    new NoneCircuitBreakerService(), mapperService);
            bitsetFilterCache = new BitsetFilterCache(idxSettings, new BitsetFilterCache.Listener() {
                @Override
                public void onCache(ShardId shardId, Accountable accountable) {

                }

                @Override
                public void onRemoval(ShardId shardId, Accountable accountable) {

                }
            });

            for (String type : currentTypes) {
                mapperService.merge(type, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(type,
                        STRING_FIELD_NAME, "type=text",
                        STRING_FIELD_NAME_2, "type=keyword",
                        INT_FIELD_NAME, "type=integer",
                        DOUBLE_FIELD_NAME, "type=double",
                        BOOLEAN_FIELD_NAME, "type=boolean",
                        DATE_FIELD_NAME, "type=date",
                        OBJECT_FIELD_NAME, "type=object",
                        GEO_POINT_FIELD_NAME, "type=geo_point",
                        GEO_SHAPE_FIELD_NAME, "type=geo_shape"
                ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
                // also add mappings for two inner field in the object field
                mapperService.merge(type, new CompressedXContent("{\"properties\":{\"" + OBJECT_FIELD_NAME + "\":{\"type\":\"object\","
                                + "\"properties\":{\"" + DATE_FIELD_NAME + "\":{\"type\":\"date\"},\"" +
                                INT_FIELD_NAME + "\":{\"type\":\"integer\"}}}}}"),
                        MapperService.MergeReason.MAPPING_UPDATE, false);
            }
            testCase.initializeAdditionalMappings(mapperService);
        }

        @Override
        public void close() throws IOException {
        }

        QueryShardContext createShardContext() {
            return new QueryShardContext(0, idxSettings, bitsetFilterCache, indexFieldDataService::getForField, mapperService,
                similarityService, scriptService, xContentRegistry, namedWriteableRegistry, this.client, null, () -> nowInMillis, null);
        }

        ScriptModule createScriptModule(List<ScriptPlugin> scriptPlugins) {
            if (scriptPlugins == null || scriptPlugins.isEmpty()) {
                return newTestScriptModule();
            }
            return new ScriptModule(Settings.EMPTY, scriptPlugins);
        }
    }

    protected QueryBuilder rewriteAndFetch(QueryBuilder builder, QueryRewriteContext context) throws IOException {
        PlainActionFuture<QueryBuilder> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(builder, context, future);
        return future.actionGet();
    }
}
