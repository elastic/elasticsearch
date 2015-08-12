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

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public abstract class BaseQueryTestCase<QB extends AbstractQueryBuilder<QB>> extends ESTestCase {

    protected static final String STRING_FIELD_NAME = "mapped_string";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_FIELD_NAME = "mapped_date";
    protected static final String OBJECT_FIELD_NAME = "mapped_object";
    protected static final String[] mappedFieldNames = new String[] { STRING_FIELD_NAME, INT_FIELD_NAME,
            DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, DATE_FIELD_NAME, OBJECT_FIELD_NAME };

    private static Injector injector;
    private static IndexQueryParserService queryParserService;
    private static Index index;

    private static String[] currentTypes;

    protected static String[] getCurrentTypes() {
        return currentTypes;
    }

    private static NamedWriteableRegistry namedWriteableRegistry;

    /**
     * Setup for the whole base test class.
     * @throws IOException
     */
    @BeforeClass
    public static void init() throws IOException {
        Settings settings = Settings.settingsBuilder()
                .put("name", BaseQueryTestCase.class.toString())
                .put("path.home", createTempDir())
                .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(),
                        Version.V_1_0_0, Version.CURRENT))
                .build();

        index = new Index("test");
        injector = new ModulesBuilder().add(
                new EnvironmentModule(new Environment(settings)),
                new SettingsModule(settings),
                new ThreadPoolModule(new ThreadPool(settings)),
                new IndicesQueriesModule(),
                new ScriptModule(settings),
                new IndexSettingsModule(index, settings),
                new IndexCacheModule(settings),
                new AnalysisModule(settings, new IndicesAnalysisService(settings)),
                new SimilarityModule(settings),
                new IndexNameModule(index),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        Multibinder.newSetBinder(binder(), ScoreFunctionParser.class);
                        bind(ClusterService.class).toProvider(Providers.of((ClusterService) null));
                        bind(CircuitBreakerService.class).to(NoneCircuitBreakerService.class);
                        bind(NamedWriteableRegistry.class).asEagerSingleton();
                    }
                }
        ).createInjector();
        queryParserService = injector.getInstance(IndexQueryParserService.class);
        MapperService mapperService = queryParserService.mapperService;
        //create some random type with some default field, those types will stick around for all of the subclasses
        currentTypes = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < currentTypes.length; i++) {
            String type = randomAsciiOfLengthBetween(1, 10);
            mapperService.merge(type, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(type,
                    STRING_FIELD_NAME, "type=string",
                    INT_FIELD_NAME, "type=integer",
                    DOUBLE_FIELD_NAME, "type=double",
                    BOOLEAN_FIELD_NAME, "type=boolean",
                    DATE_FIELD_NAME, "type=date",
                    OBJECT_FIELD_NAME, "type=object"
            ).string()), false, false);
            // also add mappings for two inner field in the object field
            mapperService.merge(type, new CompressedXContent("{\"properties\":{\""+OBJECT_FIELD_NAME+"\":{\"type\":\"object\","
                    + "\"properties\":{\""+DATE_FIELD_NAME+"\":{\"type\":\"date\"},\""+INT_FIELD_NAME+"\":{\"type\":\"integer\"}}}}}"), false, false);
            currentTypes[i] = type;
        }
        namedWriteableRegistry = injector.getInstance(NamedWriteableRegistry.class);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        terminate(injector.getInstance(ThreadPool.class));
        injector = null;
        index = null;
        queryParserService = null;
        currentTypes = null;
        namedWriteableRegistry = null;
    }

    @Before
    public void beforeTest() {
        //set some random types to be queried as part the search request, before each test
        String[] types = getRandomTypes();
        //some query (e.g. range query) have a different behaviour depending on whether the current search context is set or not
        //which is why we randomly set the search context, which will internally also do QueryParseContext.setTypes(types)
        if (randomBoolean()) {
            QueryShardContext.setTypes(types);
        } else {
            TestSearchContext testSearchContext = new TestSearchContext();
            testSearchContext.setTypes(types);
            SearchContext.setCurrent(testSearchContext);
        }
    }

    @After
    public void afterTest() {
        QueryShardContext.removeTypes();
        SearchContext.removeCurrent();
    }

    protected final QB createTestQueryBuilder() {
        QB query = doCreateTestQueryBuilder();
        if (supportsBoostAndQueryName()) {
            if (randomBoolean()) {
                query.boost(2.0f / randomIntBetween(1, 20));
            }
            if (randomBoolean()) {
                query.queryName(randomAsciiOfLengthBetween(1, 10));
            }
        }
        return query;
    }

    /**
     * Create the query that is being tested
     */
    protected abstract QB doCreateTestQueryBuilder();

    /**
     * Generic test that creates new query from the test query and checks both for equality
     * and asserts equality on the two queries.
     */
    @Test
    public void testFromXContent() throws IOException {
        QB testQuery = createTestQueryBuilder();
        QueryParseContext context = createParseContext();
        String contentString = testQuery.toString();
        XContentParser parser = XContentFactory.xContent(contentString).createParser(contentString);
        context.reset(parser);
        assertQueryHeader(parser, testQuery.getName());

        QueryBuilder newQuery = queryParserService.queryParser(testQuery.getName()).fromXContent(context);
        assertNotSame(newQuery, testQuery);
        assertEquals(testQuery, newQuery);
        assertEquals(testQuery.hashCode(), newQuery.hashCode());
    }

    /**
     * Test creates the {@link Query} from the {@link QueryBuilder} under test and delegates the
     * assertions being made on the result to the implementing subclass.
     */
    @Test
    public void testToQuery() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);

        QB firstQuery = createTestQueryBuilder();
        Query firstLuceneQuery = firstQuery.toQuery(context);
        assertLuceneQuery(firstQuery, firstLuceneQuery, context);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            firstQuery.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                QueryBuilder<? extends QueryBuilder> prototype = queryParserService.queryParser(firstQuery.getWriteableName()).getBuilderPrototype();
                @SuppressWarnings("unchecked")
                QB secondQuery = (QB)prototype.readFrom(in);
                //query _name never should affect the result of toQuery, we randomly set it to make sure
                if (randomBoolean()) {
                    secondQuery.queryName(secondQuery.queryName() == null ? randomAsciiOfLengthBetween(1, 30) : secondQuery.queryName() + randomAsciiOfLengthBetween(1, 10));
                }
                Query secondLuceneQuery = secondQuery.toQuery(context);
                assertLuceneQuery(secondQuery, secondLuceneQuery, context);
                assertThat("two equivalent query builders lead to different lucene queries", secondLuceneQuery, equalTo(firstLuceneQuery));

                //if the initial lucene query is null, changing its boost won't have any effect, we shouldn't test that
                //otherwise makes sure that boost is taken into account in toQuery
                if (firstLuceneQuery != null) {
                    secondQuery.boost(firstQuery.boost() + 1f + randomFloat());
                    //some queries don't support boost, their setter is a no-op
                    if (supportsBoostAndQueryName()) {
                        Query thirdLuceneQuery = secondQuery.toQuery(context);
                        assertThat("modifying the boost doesn't affect the corresponding lucene query", firstLuceneQuery, not(equalTo(thirdLuceneQuery)));
                    }
                }
            }
        }
    }

    /**
     * Few queries allow you to set the boost and queryName but don't do anything with it. This method allows
     * to disable boost and queryName related tests for those queries.
     */
    protected boolean supportsBoostAndQueryName() {
        return true;
    }

    /**
     * Checks the result of {@link QueryBuilder#toQuery(QueryShardContext)} given the original {@link QueryBuilder} and {@link QueryShardContext}.
     * Verifies that named queries and boost are properly handled and delegates to {@link #doAssertLuceneQuery(AbstractQueryBuilder, Query, QueryShardContext)}
     * for query specific checks.
     */
    protected final void assertLuceneQuery(QB queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedQueries().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo(query));
        }
        if (query != null) {
            assertThat(query.getBoost(), equalTo(queryBuilder.boost()));
        }
        doAssertLuceneQuery(queryBuilder, query, context);
    }

    /**
     * Checks the result of {@link QueryBuilder#toQuery(QueryShardContext)} given the original {@link QueryBuilder} and {@link QueryShardContext}.
     * Contains the query specific checks to be implemented by subclasses.
     */
    protected abstract void doAssertLuceneQuery(QB queryBuilder, Query query, QueryShardContext context) throws IOException;

    /**
     * Test serialization and deserialization of the test query.
     */
    @Test
    public void testSerialization() throws IOException {
        QB testQuery = createTestQueryBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            testQuery.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                QueryBuilder<? extends QueryBuilder> prototype = queryParserService.queryParser(testQuery.getWriteableName()).getBuilderPrototype();
                QueryBuilder deserializedQuery = prototype.readFrom(in);
                assertEquals(deserializedQuery, testQuery);
                assertEquals(deserializedQuery.hashCode(), testQuery.hashCode());
                assertNotSame(deserializedQuery, testQuery);
            }
        }
    }

    /**
     * @return a new {@link QueryShardContext} based on the base test index and queryParserService
     */
    protected static QueryShardContext createShardContext() {
        QueryShardContext queryCreationContext = new QueryShardContext(index, queryParserService);
        queryCreationContext.parseFieldMatcher(ParseFieldMatcher.EMPTY);
        return queryCreationContext;
    }

    /**
     * @return a new {@link QueryParseContext} based on the base test index and queryParserService
     */
    protected static QueryParseContext createParseContext() {
        QueryParseContext parseContext = createShardContext().parseContext();
        return parseContext;
    }

    protected static void assertQueryHeader(XContentParser parser, String expectedParserName) throws IOException {
        assertThat(parser.nextToken(), is(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), is(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), is(expectedParserName));
        assertThat(parser.nextToken(), is(XContentParser.Token.START_OBJECT));
    }

    protected static void assertValidate(QueryBuilder queryBuilder, int totalExpectedErrors) {
        QueryValidationException queryValidationException = queryBuilder.validate();
        if (totalExpectedErrors > 0) {
            assertThat(queryValidationException, notNullValue());
            assertThat(queryValidationException.validationErrors().size(), equalTo(totalExpectedErrors));
        } else {
            assertThat(queryValidationException, nullValue());
        }
    }

    /**
     * create a random value for either {@link BaseQueryTestCase#BOOLEAN_FIELD_NAME}, {@link BaseQueryTestCase#INT_FIELD_NAME},
     * {@link BaseQueryTestCase#DOUBLE_FIELD_NAME} or {@link BaseQueryTestCase#STRING_FIELD_NAME}, or a String value by default
     */
    protected static Object randomValueForField(String fieldName) {
        Object value;
        switch (fieldName) {
            case BOOLEAN_FIELD_NAME: value = randomBoolean(); break;
            case INT_FIELD_NAME: value = randomInt(); break;
            case DOUBLE_FIELD_NAME: value = randomDouble(); break;
            case STRING_FIELD_NAME: value = randomAsciiOfLengthBetween(1, 10); break;
            default : value = randomAsciiOfLengthBetween(1, 10);
        }
        return value;
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

    protected String[] getRandomTypes() {
        String[] types;
        if (currentTypes.length > 0 && randomBoolean()) {
            int numberOfQueryTypes = randomIntBetween(1, currentTypes.length);
            types = new String[numberOfQueryTypes];
            for (int i = 0; i < numberOfQueryTypes; i++) {
                types[i] = randomFrom(currentTypes);
            }
        } else {
            if (randomBoolean()) {
                types = new String[] { MetaData.ALL };
            } else {
                types = new String[0];
            }
        }
        return types;
    }

    protected String getRandomType() {
        return (currentTypes.length == 0) ? MetaData.ALL : randomFrom(currentTypes);
    }

    /**
     * Helper method to return a random field (mapped or unmapped) and a value
     */
    protected static Tuple<String, Object> getRandomFieldNameAndValue() {
        // if no type is set then return random field name and value
        if (currentTypes == null || currentTypes.length == 0) {
            return new Tuple(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 50));
        }
        // mapped fields
        String fieldName = randomFrom(mappedFieldNames);
        Object value = randomAsciiOfLengthBetween(1, 50);
        switch(fieldName) {
            case STRING_FIELD_NAME:
                value = rarely() ? randomUnicodeOfLength(10) : value; // unicode in 10% cases
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
        } // all other fields assigned to random string

        // unmapped fields
        if (randomBoolean()) {
            fieldName = randomAsciiOfLengthBetween(1, 10);
        }
        return new Tuple(fieldName, value);
    }

    protected static Fuzziness randomFuzziness(String fieldName) {
        Fuzziness fuzziness = Fuzziness.AUTO;
        switch (fieldName) {
            case INT_FIELD_NAME:
                fuzziness = Fuzziness.build(randomIntBetween(3, 100));
                break;
            case DOUBLE_FIELD_NAME:
                fuzziness = Fuzziness.build(1 + randomFloat() * 10);
                break;
            case DATE_FIELD_NAME:
                fuzziness = Fuzziness.build(randomTimeValue());
                break;
        }
        if (randomBoolean()) {
            fuzziness = Fuzziness.fromEdits(randomIntBetween(0, 2));
        }
        return fuzziness;
    }

    protected static boolean isNumericFieldName(String fieldName) {
        return INT_FIELD_NAME.equals(fieldName) || DOUBLE_FIELD_NAME.equals(fieldName);
    }
}
