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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.threadpool.ThreadPool;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public abstract class AbstractQueryTestCase<QB extends AbstractQueryBuilder<QB>> extends ESTestCase {

    public static final String STRING_FIELD_NAME = "mapped_string";
    protected static final String STRING_FIELD_NAME_2 = "mapped_string_2";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_FIELD_NAME = "mapped_date";
    protected static final String OBJECT_FIELD_NAME = "mapped_object";
    protected static final String GEO_POINT_FIELD_NAME = "mapped_geo_point";
    protected static final String GEO_POINT_FIELD_MAPPING = "type=geo_point,lat_lon=true,geohash=true,geohash_prefix=true";
    protected static final String GEO_SHAPE_FIELD_NAME = "mapped_geo_shape";
    protected static final String[] MAPPED_FIELD_NAMES = new String[]{STRING_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME,
            BOOLEAN_FIELD_NAME, DATE_FIELD_NAME, OBJECT_FIELD_NAME, GEO_POINT_FIELD_NAME, GEO_SHAPE_FIELD_NAME};
    protected static final String[] MAPPED_LEAF_FIELD_NAMES = new String[]{STRING_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME,
            BOOLEAN_FIELD_NAME, DATE_FIELD_NAME, GEO_POINT_FIELD_NAME};
    private static final int NUMBER_OF_TESTQUERIES = 20;

    private static ServiceHolder serviceHolder;
    private static int queryNameId = 0;
    private static Settings nodeSettings;
    private static Settings indexSettings;
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
        // we have to prefer CURRENT since with the range of versions we support it's rather unlikely to get the current actually.
        Version indexVersionCreated = randomBoolean() ? Version.CURRENT
                : VersionUtils.randomVersionBetween(random(), Version.V_2_0_0_beta1, Version.CURRENT);
        nodeSettings = Settings.builder()
                .put("node.name", AbstractQueryTestCase.class.toString())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false)
                .build();
        indexSettings = Settings.builder()
                .put(ParseFieldMatcher.PARSE_STRICT, true)
                .put(IndexMetaData.SETTING_VERSION_CREATED, indexVersionCreated).build();

        index = new Index(randomAsciiOfLengthBetween(1, 10), "_na_");

        //create some random type with some default field, those types will stick around for all of the subclasses
        currentTypes = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < currentTypes.length; i++) {
            String type = randomAsciiOfLengthBetween(1, 10);
            currentTypes[i] = type;
        }
        //set some random types to be queried as part the search request, before each test
        randomTypes = getRandomTypes();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        IOUtils.close(serviceHolder);
        serviceHolder = null;
    }

    @Before
    public void beforeTest() throws IOException {
        if (serviceHolder == null) {
            serviceHolder = new ServiceHolder(nodeSettings, indexSettings, getPlugins(), this);
        }
        serviceHolder.clientInvocationHandler.delegate = this;
    }

    private static void setSearchContext(String[] types, QueryShardContext context) {
        TestSearchContext testSearchContext = new TestSearchContext(context) {
            @Override
            public MapperService mapperService() {
                return serviceHolder.mapperService; // need to build / parse inner hits sort fields
            }

            @Override
            public IndexFieldDataService fieldData() {
                return serviceHolder.indexFieldDataService; // need to build / parse inner hits sort fields
            }
        };
        testSearchContext.getQueryShardContext().setTypes(types);
        SearchContext.setCurrent(testSearchContext);
    }

    @After
    public void afterTest() {
        serviceHolder.clientInvocationHandler.delegate = null;
        SearchContext.removeCurrent();
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
        String queryName = randomAsciiOfLengthBetween(1, 10) + queryNameId;
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
            XContentBuilder builder = toXContent(testQuery, randomFrom(XContentType.values()));
            XContentBuilder shuffled = shuffleXContent(builder, shuffleProtectedFields());
            assertParsedQuery(shuffled.bytes(), testQuery);
            for (Map.Entry<String, QB> alternateVersion : getAlternateVersions().entrySet()) {
                String queryAsString = alternateVersion.getKey();
                assertParsedQuery(new BytesArray(queryAsString), alternateVersion.getValue());
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

    protected static XContentBuilder toXContent(QueryBuilder query, XContentType contentType) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(contentType);
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        query.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder;
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
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(queryAsString));
        // we'd like to see the offending field name here
        assertThat(e.getMessage(), containsString("bogusField"));
    }

    /**
     * Test that adding additional object into otherwise correct query string
     * should always trigger some kind of Parsing Exception.
     */
    public void testUnknownObjectException() throws IOException {
        String validQuery = createTestQueryBuilder().toString();
        assertThat(validQuery, containsString("{"));
        for (int insertionPosition = 0; insertionPosition < validQuery.length(); insertionPosition++) {
            if (validQuery.charAt(insertionPosition) == '{') {
                String testQuery = validQuery.substring(0, insertionPosition) + "{ \"newField\" : "
                        + validQuery.substring(insertionPosition) + "}";
                try {
                    parseQuery(testQuery);
                    fail("some parsing exception expected for query: " + testQuery);
                } catch (ParsingException | ElasticsearchParseException e) {
                    // different kinds of exception wordings depending on location
                    // of mutation, so no simple asserts possible here
                } catch (JsonParseException e) {
                    // mutation produced invalid json
                }
            }
        }
    }

    /**
     * Test that wraps the randomly generated query into an array as follows: { "query_name" : [{}]}
     * This causes unexpected situations in parser code that may not be handled properly.
     */
    public void testQueryWrappedInArray() throws IOException {
        QB queryBuilder = createTestQueryBuilder();
        String validQuery = queryBuilder.toString();
        String queryName = queryBuilder.getName();
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
    protected static void assertParsedQuery(String queryAsString, QueryBuilder expectedQuery) throws IOException {
        assertParsedQuery(queryAsString, expectedQuery, ParseFieldMatcher.STRICT);
    }

    protected static void assertParsedQuery(String queryAsString, QueryBuilder expectedQuery, ParseFieldMatcher matcher)
            throws IOException {
        QueryBuilder newQuery = parseQuery(queryAsString, matcher);
        assertNotSame(newQuery, expectedQuery);
        assertEquals(expectedQuery, newQuery);
        assertEquals(expectedQuery.hashCode(), newQuery.hashCode());
    }

    /**
     * Parses the query provided as bytes argument and compares it with the expected result provided as argument as a {@link QueryBuilder}
     */
    private static void assertParsedQuery(BytesReference queryAsBytes, QueryBuilder expectedQuery) throws IOException {
        assertParsedQuery(queryAsBytes, expectedQuery, ParseFieldMatcher.STRICT);
    }

    private static void assertParsedQuery(BytesReference queryAsBytes, QueryBuilder expectedQuery, ParseFieldMatcher matcher)
            throws IOException {
        QueryBuilder newQuery = parseQuery(queryAsBytes, matcher);
        assertNotSame(newQuery, expectedQuery);
        assertEquals(expectedQuery, newQuery);
        assertEquals(expectedQuery.hashCode(), newQuery.hashCode());
    }

    protected static QueryBuilder parseQuery(String queryAsString) throws IOException {
        return parseQuery(queryAsString, ParseFieldMatcher.STRICT);
    }

    protected static QueryBuilder parseQuery(String queryAsString, ParseFieldMatcher matcher) throws IOException {
        XContentParser parser = XContentFactory.xContent(queryAsString).createParser(queryAsString);
        return parseQuery(parser, matcher);
    }

    protected static QueryBuilder parseQuery(BytesReference queryAsBytes) throws IOException {
        return parseQuery(queryAsBytes, ParseFieldMatcher.STRICT);
    }

    protected static QueryBuilder parseQuery(BytesReference queryAsBytes, ParseFieldMatcher matcher) throws IOException {
        XContentParser parser = XContentFactory.xContent(queryAsBytes).createParser(queryAsBytes);
        return parseQuery(parser, matcher);
    }

    private static QueryBuilder parseQuery(XContentParser parser, ParseFieldMatcher matcher) throws IOException {
        QueryParseContext context = createParseContext(parser, matcher);
        QueryBuilder parseInnerQueryBuilder = context.parseInnerQueryBuilder()
                .orElseThrow(() -> new IllegalArgumentException("inner query body cannot be empty"));
        assertNull(parser.nextToken());
        return parseInnerQueryBuilder;
    }

    /**
     * Test creates the {@link Query} from the {@link QueryBuilder} under test and delegates the
     * assertions being made on the result to the implementing subclass.
     */
    public void testToQuery() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTQUERIES; runs++) {
            QueryShardContext context = createShardContext();
            context.setAllowUnmappedFields(true);
            QB firstQuery = createTestQueryBuilder();
            QB controlQuery = copyQuery(firstQuery);
            setSearchContext(randomTypes, context); // only set search context for toQuery to be more realistic
            Query firstLuceneQuery = rewriteQuery(firstQuery, context).toQuery(context);
            assertNotNull("toQuery should not return null", firstLuceneQuery);
            assertLuceneQuery(firstQuery, firstLuceneQuery, context);
            //remove after assertLuceneQuery since the assertLuceneQuery impl might access the context as well
            SearchContext.removeCurrent();
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
                secondQuery.queryName(secondQuery.queryName() == null ? randomAsciiOfLengthBetween(1, 30) : secondQuery.queryName()
                        + randomAsciiOfLengthBetween(1, 10));
            }
            setSearchContext(randomTypes, context);
            Query secondLuceneQuery = rewriteQuery(secondQuery, context).toQuery(context);
            assertNotNull("toQuery should not return null", secondLuceneQuery);
            assertLuceneQuery(secondQuery, secondLuceneQuery, context);
            SearchContext.removeCurrent();

            assertEquals("two equivalent query builders lead to different lucene queries",
                    rewrite(secondLuceneQuery), rewrite(firstLuceneQuery));

            if (supportsBoostAndQueryName()) {
                secondQuery.boost(firstQuery.boost() + 1f + randomFloat());
                setSearchContext(randomTypes, context);
                Query thirdLuceneQuery = rewriteQuery(secondQuery, context).toQuery(context);
                SearchContext.removeCurrent();
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
        QueryBuilder rewritten = QueryBuilder.rewriteQuery(queryBuilder, rewriteContext);
        // extra safety to fail fast - serialize the rewritten version to ensure it's serializable.
        assertSerialization(rewritten);
        return rewritten;
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
     * {@link #doAssertLuceneQuery(AbstractQueryBuilder, Query, QueryShardContext)} for query specific checks.
     */
    private void assertLuceneQuery(QB queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedQueries().get(queryBuilder.queryName());
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
    protected abstract void doAssertLuceneQuery(QB queryBuilder, Query query, QueryShardContext context) throws IOException;

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

    /**
     * Serialize the given query builder and asserts that both are equal
     */
    protected static QueryBuilder assertSerialization(QueryBuilder testQuery) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(testQuery);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), serviceHolder.namedWriteableRegistry)) {
                QueryBuilder deserializedQuery = in.readNamedWriteable(QueryBuilder.class);
                assertEquals(testQuery, deserializedQuery);
                assertEquals(testQuery.hashCode(), deserializedQuery.hashCode());
                assertNotSame(testQuery, deserializedQuery);
                return deserializedQuery;
            }
        }
    }

    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTQUERIES; runs++) {
            QB firstQuery = createTestQueryBuilder();
            assertFalse("query is equal to null", firstQuery.equals(null));
            assertFalse("query is equal to incompatible type", firstQuery.equals(""));
            assertTrue("query is not equal to self", firstQuery.equals(firstQuery));
            assertThat("same query's hashcode returns different values if called multiple times", firstQuery.hashCode(),
                    equalTo(firstQuery.hashCode()));

            QB secondQuery = copyQuery(firstQuery);
            assertTrue("query is not equal to self", secondQuery.equals(secondQuery));
            assertTrue("query is not equal to its copy", firstQuery.equals(secondQuery));
            assertTrue("equals is not symmetric", secondQuery.equals(firstQuery));
            assertThat("query copy's hashcode is different from original hashcode", secondQuery.hashCode(), equalTo(firstQuery.hashCode()));

            QB thirdQuery = copyQuery(secondQuery);
            assertTrue("query is not equal to self", thirdQuery.equals(thirdQuery));
            assertTrue("query is not equal to its copy", secondQuery.equals(thirdQuery));
            assertThat("query copy's hashcode is different from original hashcode", secondQuery.hashCode(), equalTo(thirdQuery.hashCode()));
            assertTrue("equals is not transitive", firstQuery.equals(thirdQuery));
            assertThat("query copy's hashcode is different from original hashcode", firstQuery.hashCode(), equalTo(thirdQuery.hashCode()));
            assertTrue("equals is not symmetric", thirdQuery.equals(secondQuery));
            assertTrue("equals is not symmetric", thirdQuery.equals(firstQuery));

            if (randomBoolean()) {
                secondQuery.queryName(secondQuery.queryName() == null ? randomAsciiOfLengthBetween(1, 30) : secondQuery.queryName()
                        + randomAsciiOfLengthBetween(1, 10));
            } else {
                secondQuery.boost(firstQuery.boost() + 1f + randomFloat());
            }
            assertThat("different queries should not be equal", secondQuery, not(equalTo(firstQuery)));
        }
    }

    //we use the streaming infra to create a copy of the query provided as argument
    @SuppressWarnings("unchecked")
    private QB copyQuery(QB query) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(query);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), serviceHolder.namedWriteableRegistry)) {
                return (QB) in.readNamedWriteable(QueryBuilder.class);
            }
        }
    }

    /**
     * @return a new {@link QueryShardContext} based on the base test index and queryParserService
     */
    protected static QueryShardContext createShardContext() {
        return serviceHolder.createShardContext();
    }

    /**
     * @return a new {@link QueryParseContext} based on the base test index and queryParserService
     */
    protected static QueryParseContext createParseContext(XContentParser parser, ParseFieldMatcher matcher) {
        return new QueryParseContext(serviceHolder.indicesQueriesRegistry, parser, matcher);
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
                    value = randomAsciiOfLengthBetween(1, 10);
                }
                break;
            case INT_FIELD_NAME:
                value = randomIntBetween(0, 10);
                break;
            case DOUBLE_FIELD_NAME:
                value = randomDouble() * 10;
                break;
            case BOOLEAN_FIELD_NAME:
                value = randomBoolean();
                break;
            case DATE_FIELD_NAME:
                value = new DateTime(System.currentTimeMillis(), DateTimeZone.UTC).toString();
                break;
            default:
                value = randomAsciiOfLengthBetween(1, 10);
        }
        return value;
    }

    protected static String getRandomQueryText() {
        int terms = randomIntBetween(0, 3);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < terms; i++) {
            builder.append(randomAsciiOfLengthBetween(1, 10)).append(" ");
        }
        return builder.toString().trim();
    }

    /**
     * Helper method to return a mapped or a random field
     */
    protected static String getRandomFieldName() {
        // if no type is set then return a random field name
        if (currentTypes.length == 0 || randomBoolean()) {
            return randomAsciiOfLengthBetween(1, 10);
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

    protected static String getRandomType() {
        return (currentTypes.length == 0) ? MetaData.ALL : randomFrom(currentTypes);
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
            if (method.equals(Client.class.getMethod("get", GetRequest.class))) {
                return new PlainActionFuture<GetResponse>() {
                    @Override
                    public GetResponse get() throws InterruptedException, ExecutionException {
                        return delegate.executeGet((GetRequest) args[0]);
                    }
                };
            } else if (method.equals(Client.class.getMethod("multiTermVectors", MultiTermVectorsRequest.class))) {
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
     * <li> Put the manual checks into the asserQueryParsedFromJson method.
     * <li> Now copy the generated json including default options into createParseableQueryJso
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
        setSearchContext(randomTypes, context); // only set search context for toQuery to be more realistic
        queryBuilder.toQuery(context);
    }

    protected Query rewrite(Query query) throws IOException {
        return query;
    }

    private static class ServiceHolder implements Closeable {

        private final Injector injector;
        private final IndicesQueriesRegistry indicesQueriesRegistry;
        private final IndexFieldDataService indexFieldDataService;
        private final SearchModule searchModule;
        private final NamedWriteableRegistry namedWriteableRegistry;
        private final ClientInvocationHandler clientInvocationHandler = new ClientInvocationHandler();
        private final IndexSettings idxSettings;
        private final SimilarityService similarityService;
        private final MapperService mapperService;
        private final BitsetFilterCache bitsetFilterCache;
        private final ScriptService scriptService;

        ServiceHolder(Settings nodeSettings, Settings indexSettings,
                      Collection<Class<? extends Plugin>> plugins, AbstractQueryTestCase<?> testCase) throws IOException {
            final ThreadPool threadPool = new ThreadPool(nodeSettings);
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
            ClusterServiceUtils.setState(clusterService, new ClusterState.Builder(clusterService.state()).metaData(
                            new MetaData.Builder().put(new IndexMetaData.Builder(
                                    index.getName()).settings(indexSettings).numberOfShards(1).numberOfReplicas(0))));
            Environment env = InternalSettingsPreparer.prepareEnvironment(nodeSettings, null);
            PluginsService pluginsService = new PluginsService(nodeSettings, env.modulesFile(), env.pluginsFile(), plugins);

            final Client proxy = (Client) Proxy.newProxyInstance(
                    Client.class.getClassLoader(),
                    new Class[]{Client.class},
                    clientInvocationHandler);
            ScriptModule scriptModule = createScriptModule(pluginsService.filterPlugins(ScriptPlugin.class));
            List<Setting<?>> scriptSettings = scriptModule.getSettings();
            scriptSettings.addAll(pluginsService.getPluginSettings());
            scriptSettings.add(InternalSettingsPlugin.VERSION_CREATED);
            SettingsModule settingsModule = new SettingsModule(nodeSettings, scriptSettings, pluginsService.getPluginSettingsFilter());
            searchModule = new SearchModule(nodeSettings, false, pluginsService.filterPlugins(SearchPlugin.class)) {
                @Override
                protected void configureSearch() {
                    // Skip me
                }
            };
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class)) {
                @Override
                public void configure() {
                    // skip services
                    bindMapperExtension();
                }
            };
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
            entries.addAll(indicesModule.getNamedWriteables());
            entries.addAll(searchModule.getNamedWriteables());
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(entries);
            ModulesBuilder modulesBuilder = new ModulesBuilder();
            for (Module pluginModule : pluginsService.createGuiceModules()) {
                modulesBuilder.add(pluginModule);
            }
            modulesBuilder.add(
                    b -> {
                        b.bind(PluginsService.class).toInstance(pluginsService);
                        b.bind(Environment.class).toInstance(new Environment(nodeSettings));
                        b.bind(ThreadPool.class).toInstance(threadPool);
                        b.bind(Client.class).toInstance(proxy);
                        b.bind(ClusterService.class).toProvider(Providers.of(clusterService));
                        b.bind(CircuitBreakerService.class).to(NoneCircuitBreakerService.class);
                        b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
                    },
                    settingsModule, indicesModule, searchModule, new IndexSettingsModule(index, indexSettings)
            );
            pluginsService.processModules(modulesBuilder);
            injector = modulesBuilder.createInjector();
            IndexScopedSettings indexScopedSettings = injector.getInstance(IndexScopedSettings.class);
            idxSettings = IndexSettingsModule.newIndexSettings(index, indexSettings, indexScopedSettings);
            AnalysisModule analysisModule = new AnalysisModule(new Environment(nodeSettings), emptyList());
            AnalysisService analysisService = analysisModule.getAnalysisRegistry().build(idxSettings);
            scriptService = scriptModule.getScriptService();
            similarityService = new SimilarityService(idxSettings, Collections.emptyMap());
            MapperRegistry mapperRegistry = injector.getInstance(MapperRegistry.class);
            mapperService = new MapperService(idxSettings, analysisService, similarityService, mapperRegistry, this::createShardContext);
            IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(nodeSettings, new IndexFieldDataCache.Listener() {
            });
            indexFieldDataService = new IndexFieldDataService(idxSettings, indicesFieldDataCache,
                    injector.getInstance(CircuitBreakerService.class), mapperService);
            bitsetFilterCache = new BitsetFilterCache(idxSettings, new BitsetFilterCache.Listener() {
                @Override
                public void onCache(ShardId shardId, Accountable accountable) {

                }

                @Override
                public void onRemoval(ShardId shardId, Accountable accountable) {

                }
            });
            indicesQueriesRegistry = injector.getInstance(IndicesQueriesRegistry.class);

            for (String type : currentTypes) {
                mapperService.merge(type, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(type,
                        STRING_FIELD_NAME, "type=text",
                        STRING_FIELD_NAME_2, "type=keyword",
                        INT_FIELD_NAME, "type=integer",
                        DOUBLE_FIELD_NAME, "type=double",
                        BOOLEAN_FIELD_NAME, "type=boolean",
                        DATE_FIELD_NAME, "type=date",
                        OBJECT_FIELD_NAME, "type=object",
                        GEO_POINT_FIELD_NAME, GEO_POINT_FIELD_MAPPING,
                        GEO_SHAPE_FIELD_NAME, "type=geo_shape"
                ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
                // also add mappings for two inner field in the object field
                mapperService.merge(type, new CompressedXContent("{\"properties\":{\"" + OBJECT_FIELD_NAME + "\":{\"type\":\"object\","
                                + "\"properties\":{\"" + DATE_FIELD_NAME + "\":{\"type\":\"date\"},\"" +
                                INT_FIELD_NAME + "\":{\"type\":\"integer\"}}}}}"),
                        MapperService.MergeReason.MAPPING_UPDATE, false);
            }
            testCase.initializeAdditionalMappings(mapperService);
            this.namedWriteableRegistry = injector.getInstance(NamedWriteableRegistry.class);
        }

        @Override
        public void close() throws IOException {
            injector.getInstance(ClusterService.class).close();
            try {
                terminate(injector.getInstance(ThreadPool.class));
            } catch (InterruptedException e) {
                IOUtils.reThrow(e);
            }
        }

        QueryShardContext createShardContext() {
            ClusterState state = ClusterState.builder(new ClusterName("_name")).build();
            Client client = injector.getInstance(Client.class);
            return new QueryShardContext(idxSettings, bitsetFilterCache, indexFieldDataService, mapperService, similarityService,
                    scriptService, indicesQueriesRegistry, client, null, state);
        }

        ScriptModule createScriptModule(List<ScriptPlugin> scriptPlugins) {
            if (scriptPlugins == null || scriptPlugins.isEmpty()) {
                return newTestScriptModule();
            }

            Settings settings = Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                    // no file watching, so we don't need a ResourceWatcherService
                    .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false)
                    .build();
            Environment environment = new Environment(settings);
            return ScriptModule.create(settings, environment, null, scriptPlugins);
        }
    }
}
