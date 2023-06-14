/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rescore;

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.rescore.QueryRescorer.QueryRescoreContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.containsString;

public class QueryRescorerBuilderTests extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;
    private static NamedXContentRegistry xContentRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
        xContentRegistry = null;
    }

    /**
     * Test serialization and deserialization of the rescore builder
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            RescorerBuilder<?> original = randomRescoreBuilder();
            RescorerBuilder<?> deserialized = copy(original);
            assertEquals(deserialized, original);
            assertEquals(deserialized.hashCode(), original.hashCode());
            assertNotSame(deserialized, original);
        }
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            checkEqualsAndHashCode(randomRescoreBuilder(), this::copy, QueryRescorerBuilderTests::mutate);
        }
    }

    private RescorerBuilder<?> copy(RescorerBuilder<?> original) throws IOException {
        return copyWriteable(
            original,
            namedWriteableRegistry,
            namedWriteableRegistry.getReader(RescorerBuilder.class, original.getWriteableName())
        );
    }

    /**
     *  creates random rescorer, renders it to xContent and back to new instance that should be equal to original
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            RescorerBuilder<?> rescoreBuilder = randomRescoreBuilder();
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            rescoreBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            XContentBuilder shuffled = shuffleXContent(builder);

            try (XContentParser parser = createParser(shuffled)) {
                parser.nextToken();
                RescorerBuilder<?> secondRescoreBuilder = RescorerBuilder.parseFromXContent(parser);
                assertNotSame(rescoreBuilder, secondRescoreBuilder);
                assertEquals(rescoreBuilder, secondRescoreBuilder);
                assertEquals(rescoreBuilder.hashCode(), secondRescoreBuilder.hashCode());
            }
        }
    }

    /**
     * test that build() outputs a {@link RescoreContext} that has the same properties
     * than the test builder
     */
    public void testBuildRescoreSearchContext() throws ElasticsearchParseException, IOException {
        final long nowInMillis = randomNonNegativeLong();
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAlphaOfLengthBetween(1, 10), indexSettings);
        // shard context will only need indicesQueriesRegistry for building Query objects nested in query rescorer
        SearchExecutionContext mockContext = new SearchExecutionContext(
            0,
            0,
            idxSettings,
            null,
            null,
            null,
            null,
            MappingLookup.EMPTY,
            null,
            null,
            parserConfig(),
            namedWriteableRegistry,
            null,
            null,
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null,
            emptyMap()
        ) {
            @Override
            public MappedFieldType getFieldType(String name) {
                TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name, createDefaultIndexAnalyzers());
                return builder.build(MapperBuilderContext.root(false)).fieldType();
            }
        };

        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            QueryRescorerBuilder rescoreBuilder = randomRescoreBuilder();
            QueryRescoreContext rescoreContext = (QueryRescoreContext) rescoreBuilder.buildContext(mockContext);
            int expectedWindowSize = rescoreBuilder.windowSize() == null
                ? RescorerBuilder.DEFAULT_WINDOW_SIZE
                : rescoreBuilder.windowSize().intValue();
            assertEquals(expectedWindowSize, rescoreContext.getWindowSize());
            Query expectedQuery = Rewriteable.rewrite(rescoreBuilder.getRescoreQuery(), mockContext).toQuery(mockContext);
            assertEquals(expectedQuery, rescoreContext.parsedQuery().query());
            assertEquals(rescoreBuilder.getQueryWeight(), rescoreContext.queryWeight(), Float.MIN_VALUE);
            assertEquals(rescoreBuilder.getRescoreQueryWeight(), rescoreContext.rescoreQueryWeight(), Float.MIN_VALUE);
            assertEquals(rescoreBuilder.getScoreMode(), rescoreContext.scoreMode());
        }
    }

    public void testRescoreQueryNull() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new QueryRescorerBuilder((QueryBuilder) null));
        assertEquals("rescore_query cannot be null", e.getMessage());
    }

    class AlwaysRewriteQueryBuilder extends MatchAllQueryBuilder {

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
            return new MatchAllQueryBuilder();
        }
    }

    public void testRewritingKeepsSettings() throws IOException {

        final long nowInMillis = randomNonNegativeLong();
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAlphaOfLengthBetween(1, 10), indexSettings);
        // shard context will only need indicesQueriesRegistry for building Query objects nested in query rescorer
        SearchExecutionContext mockContext = new SearchExecutionContext(
            0,
            0,
            idxSettings,
            null,
            null,
            null,
            null,
            MappingLookup.EMPTY,
            null,
            null,
            parserConfig(),
            namedWriteableRegistry,
            null,
            null,
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null,
            emptyMap()
        ) {
            @Override
            public MappedFieldType getFieldType(String name) {
                TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name, createDefaultIndexAnalyzers());
                return builder.build(MapperBuilderContext.root(false)).fieldType();
            }
        };

        QueryBuilder rewriteQb = new AlwaysRewriteQueryBuilder();
        org.elasticsearch.search.rescore.QueryRescorerBuilder rescoreBuilder = new org.elasticsearch.search.rescore.QueryRescorerBuilder(
            rewriteQb
        );

        rescoreBuilder.setQueryWeight(randomFloat());
        rescoreBuilder.setRescoreQueryWeight(randomFloat());
        rescoreBuilder.setScoreMode(QueryRescoreMode.Max);
        rescoreBuilder.windowSize(randomIntBetween(0, 100));

        QueryRescorerBuilder rescoreRewritten = rescoreBuilder.rewrite(mockContext);
        assertEquals(rescoreRewritten.getQueryWeight(), rescoreBuilder.getQueryWeight(), 0.01f);
        assertEquals(rescoreRewritten.getRescoreQueryWeight(), rescoreBuilder.getRescoreQueryWeight(), 0.01f);
        assertEquals(rescoreRewritten.getScoreMode(), rescoreBuilder.getScoreMode());
        assertEquals(rescoreRewritten.windowSize(), rescoreBuilder.windowSize());
    }

    /**
     * test parsing exceptions for incorrect rescorer syntax
     */
    public void testUnknownFieldsExpection() throws IOException {

        String rescoreElement = """
            {
                "window_size" : 20,
                "bad_rescorer_name" : { }
            }
            """;
        try (XContentParser parser = createParser(rescoreElement)) {
            Exception e = expectThrows(NamedObjectNotFoundException.class, () -> RescorerBuilder.parseFromXContent(parser));
            assertEquals("[3:27] unknown field [bad_rescorer_name]", e.getMessage());
        }
        rescoreElement = """
            {
                "bad_fieldName" : 20
            }
            """;
        try (XContentParser parser = createParser(rescoreElement)) {
            Exception e = expectThrows(ParsingException.class, () -> RescorerBuilder.parseFromXContent(parser));
            assertEquals("rescore doesn't support [bad_fieldName]", e.getMessage());
        }

        rescoreElement = """
            {
                "window_size" : 20,
                "query" : [ ]
            }
            """;
        try (XContentParser parser = createParser(rescoreElement)) {
            Exception e = expectThrows(ParsingException.class, () -> RescorerBuilder.parseFromXContent(parser));
            assertEquals("unexpected token [START_ARRAY] after [query]", e.getMessage());
        }

        rescoreElement = "{ }";
        try (XContentParser parser = createParser(rescoreElement)) {
            Exception e = expectThrows(ParsingException.class, () -> RescorerBuilder.parseFromXContent(parser));
            assertEquals("missing rescore type", e.getMessage());
        }

        rescoreElement = """
            {
                "window_size" : 20,
                "query" : { "bad_fieldname" : 1.0  }\s
            }
            """;
        try (XContentParser parser = createParser(rescoreElement)) {
            XContentParseException e = expectThrows(XContentParseException.class, () -> RescorerBuilder.parseFromXContent(parser));
            assertEquals("[3:17] [query] unknown field [bad_fieldname]", e.getMessage());
        }

        rescoreElement = """
            {
                "window_size" : 20,
                "query" : { "rescore_query" : { "unknown_queryname" : { } } }\s
            }
            """;
        try (XContentParser parser = createParser(rescoreElement)) {
            Exception e = expectThrows(XContentParseException.class, () -> RescorerBuilder.parseFromXContent(parser));
            assertThat(e.getMessage(), containsString("[query] failed to parse field [rescore_query]"));
        }

        rescoreElement = """
            {
                "window_size" : 20,
                "query" : { "rescore_query" : { "match_all" : { } } }\s
            }
            """;
        try (XContentParser parser = createParser(rescoreElement)) {
            RescorerBuilder.parseFromXContent(parser);
        }
    }

    /**
     * create a new parser from the rescorer string representation and reset context with it
     */
    private XContentParser createParser(String rescoreElement) throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, rescoreElement);
        // move to first token, this is where the internal fromXContent
        assertTrue(parser.nextToken() == XContentParser.Token.START_OBJECT);
        return parser;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    private static RescorerBuilder<?> mutate(RescorerBuilder<?> original) throws IOException {
        RescorerBuilder<?> mutation = ESTestCase.copyWriteable(original, namedWriteableRegistry, QueryRescorerBuilder::new);
        if (randomBoolean()) {
            Integer windowSize = original.windowSize();
            if (windowSize != null) {
                mutation.windowSize(windowSize + 1);
            } else {
                mutation.windowSize(randomIntBetween(0, 100));
            }
        } else {
            QueryRescorerBuilder queryRescorer = (QueryRescorerBuilder) mutation;
            switch (randomIntBetween(0, 3)) {
                case 0 -> queryRescorer.setQueryWeight(queryRescorer.getQueryWeight() + 0.1f);
                case 1 -> queryRescorer.setRescoreQueryWeight(queryRescorer.getRescoreQueryWeight() + 0.1f);
                case 2 -> {
                    QueryRescoreMode other;
                    do {
                        other = randomFrom(QueryRescoreMode.values());
                    } while (other == queryRescorer.getScoreMode());
                    queryRescorer.setScoreMode(other);
                }
                case 3 ->
                    // only increase the boost to make it a slightly different query
                    queryRescorer.getRescoreQuery().boost(queryRescorer.getRescoreQuery().boost() + 0.1f);
                default -> throw new IllegalStateException("unexpected random mutation in test");
            }
        }
        return mutation;
    }

    /**
     * create random shape that is put under test
     */
    public static QueryRescorerBuilder randomRescoreBuilder() {
        QueryBuilder queryBuilder = new MatchAllQueryBuilder().boost(randomFloat()).queryName(randomAlphaOfLength(20));
        org.elasticsearch.search.rescore.QueryRescorerBuilder rescorer = new org.elasticsearch.search.rescore.QueryRescorerBuilder(
            queryBuilder
        );
        if (randomBoolean()) {
            rescorer.setQueryWeight(randomFloat());
        }
        if (randomBoolean()) {
            rescorer.setRescoreQueryWeight(randomFloat());
        }
        if (randomBoolean()) {
            rescorer.setScoreMode(randomFrom(QueryRescoreMode.values()));
        }
        if (randomBoolean()) {
            rescorer.windowSize(randomIntBetween(0, 100));
        }
        return rescorer;
    }
}
