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

package org.elasticsearch.search.rescore;

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.core.TextFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.rescore.QueryRescorer.QueryRescoreContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class QueryRescoreBuilderTests extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;
    private static IndicesQueriesRegistry indicesQueriesRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        indicesQueriesRegistry = searchModule.getQueryParserRegistry();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
        indicesQueriesRegistry = null;
    }

    /**
     * Test serialization and deserialization of the rescore builder
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            RescoreBuilder<?> original = randomRescoreBuilder();
            RescoreBuilder<?> deserialized = serializedCopy(original);
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
            RescoreBuilder<?> firstBuilder = randomRescoreBuilder();
            assertFalse("rescore builder is equal to null", firstBuilder.equals(null));
            assertFalse("rescore builder is equal to incompatible type", firstBuilder.equals(""));
            assertTrue("rescore builder is not equal to self", firstBuilder.equals(firstBuilder));
            assertThat("same rescore builder's hashcode returns different values if called multiple times", firstBuilder.hashCode(),
                    equalTo(firstBuilder.hashCode()));
            assertThat("different rescore builder should not be equal", mutate(firstBuilder), not(equalTo(firstBuilder)));

            RescoreBuilder<?> secondBuilder = serializedCopy(firstBuilder);
            assertTrue("rescore builder is not equal to self", secondBuilder.equals(secondBuilder));
            assertTrue("rescore builder is not equal to its copy", firstBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", secondBuilder.equals(firstBuilder));
            assertThat("rescore builder copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
                    equalTo(firstBuilder.hashCode()));

            RescoreBuilder<?> thirdBuilder = serializedCopy(secondBuilder);
            assertTrue("rescore builder is not equal to self", thirdBuilder.equals(thirdBuilder));
            assertTrue("rescore builder is not equal to its copy", secondBuilder.equals(thirdBuilder));
            assertThat("rescore builder copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
                    equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not transitive", firstBuilder.equals(thirdBuilder));
            assertThat("rescore builder copy's hashcode is different from original hashcode", firstBuilder.hashCode(),
                    equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not symmetric", thirdBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", thirdBuilder.equals(firstBuilder));
        }
    }

    /**
     *  creates random rescorer, renders it to xContent and back to new instance that should be equal to original
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            RescoreBuilder<?> rescoreBuilder = randomRescoreBuilder();
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            rescoreBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            XContentBuilder shuffled = shuffleXContent(builder);


            XContentParser parser = XContentHelper.createParser(shuffled.bytes());
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.STRICT);
            parser.nextToken();
            RescoreBuilder<?> secondRescoreBuilder = RescoreBuilder.parseFromXContent(context);
            assertNotSame(rescoreBuilder, secondRescoreBuilder);
            assertEquals(rescoreBuilder, secondRescoreBuilder);
            assertEquals(rescoreBuilder.hashCode(), secondRescoreBuilder.hashCode());
        }
    }

    /**
     * test that build() outputs a {@link RescoreSearchContext} that has the same properties
     * than the test builder
     */
    public void testBuildRescoreSearchContext() throws ElasticsearchParseException, IOException {
        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAsciiOfLengthBetween(1, 10), indexSettings);
        // shard context will only need indicesQueriesRegistry for building Query objects nested in query rescorer
        QueryShardContext mockShardContext = new QueryShardContext(idxSettings, null, null, null, null, null, indicesQueriesRegistry,
                null, null, null) {
            @Override
            public MappedFieldType fieldMapper(String name) {
                TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name);
                return builder.build(new Mapper.BuilderContext(idxSettings.getSettings(), new ContentPath(1))).fieldType();
            }
        };

        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            QueryRescorerBuilder rescoreBuilder = randomRescoreBuilder();
            QueryRescoreContext rescoreContext = rescoreBuilder.build(mockShardContext);
            int expectedWindowSize = rescoreBuilder.windowSize() == null ? QueryRescoreContext.DEFAULT_WINDOW_SIZE :
                rescoreBuilder.windowSize().intValue();
            assertEquals(expectedWindowSize, rescoreContext.window());
            Query expectedQuery = QueryBuilder.rewriteQuery(rescoreBuilder.getRescoreQuery(), mockShardContext).toQuery(mockShardContext);
            assertEquals(expectedQuery, rescoreContext.query());
            assertEquals(rescoreBuilder.getQueryWeight(), rescoreContext.queryWeight(), Float.MIN_VALUE);
            assertEquals(rescoreBuilder.getRescoreQueryWeight(), rescoreContext.rescoreQueryWeight(), Float.MIN_VALUE);
            assertEquals(rescoreBuilder.getScoreMode(), rescoreContext.scoreMode());
        }
    }

    public void testRescoreQueryNull() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new QueryRescorerBuilder((QueryBuilder) null));
        assertEquals("rescore_query cannot be null", e.getMessage());
    }

    /**
     * test parsing exceptions for incorrect rescorer syntax
     */
    public void testUnknownFieldsExpection() throws IOException {

        String rescoreElement = "{\n" +
                "    \"window_size\" : 20,\n" +
                "    \"bad_rescorer_name\" : { }\n" +
                "}\n";
        QueryParseContext context = createContext(rescoreElement);
        try {
            RescoreBuilder.parseFromXContent(context);
            fail("expected a parsing exception");
        } catch (ParsingException e) {
            assertEquals("rescore doesn't support rescorer with name [bad_rescorer_name]", e.getMessage());
        }

        rescoreElement = "{\n" +
                "    \"bad_fieldName\" : 20\n" +
                "}\n";
        context = createContext(rescoreElement);
        try {
            RescoreBuilder.parseFromXContent(context);
            fail("expected a parsing exception");
        } catch (ParsingException e) {
            assertEquals("rescore doesn't support [bad_fieldName]", e.getMessage());
        }

        rescoreElement = "{\n" +
                "    \"window_size\" : 20,\n" +
                "    \"query\" : [ ]\n" +
                "}\n";
        context = createContext(rescoreElement);
        try {
            RescoreBuilder.parseFromXContent(context);
            fail("expected a parsing exception");
        } catch (ParsingException e) {
            assertEquals("unexpected token [START_ARRAY] after [query]", e.getMessage());
        }

        rescoreElement = "{ }";
        context = createContext(rescoreElement);
        try {
            RescoreBuilder.parseFromXContent(context);
            fail("expected a parsing exception");
        } catch (ParsingException e) {
            assertEquals("missing rescore type", e.getMessage());
        }

        rescoreElement = "{\n" +
                "    \"window_size\" : 20,\n" +
                "    \"query\" : { \"bad_fieldname\" : 1.0  } \n" +
                "}\n";
        context = createContext(rescoreElement);
        try {
            RescoreBuilder.parseFromXContent(context);
            fail("expected a parsing exception");
        } catch (IllegalArgumentException e) {
            assertEquals("[query] unknown field [bad_fieldname], parser not found", e.getMessage());
        }

        rescoreElement = "{\n" +
                "    \"window_size\" : 20,\n" +
                "    \"query\" : { \"rescore_query\" : { \"unknown_queryname\" : { } } } \n" +
                "}\n";
        context = createContext(rescoreElement);
        try {
            RescoreBuilder.parseFromXContent(context);
            fail("expected a parsing exception");
        } catch (ParsingException e) {
            assertEquals("[query] failed to parse field [rescore_query]", e.getMessage());
        }

        rescoreElement = "{\n" +
                "    \"window_size\" : 20,\n" +
                "    \"query\" : { \"rescore_query\" : { \"match_all\" : { } } } \n"
                + "}\n";
        context = createContext(rescoreElement);
        RescoreBuilder.parseFromXContent(context);
    }

    /**
     * create a new parser from the rescorer string representation and reset context with it
     */
    private static QueryParseContext createContext(String rescoreElement) throws IOException {
        XContentParser parser = XContentFactory.xContent(rescoreElement).createParser(rescoreElement);
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.STRICT);
        // move to first token, this is where the internal fromXContent
        assertTrue(parser.nextToken() == XContentParser.Token.START_OBJECT);
        return context;
    }

    private static RescoreBuilder<?> mutate(RescoreBuilder<?> original) throws IOException {
        RescoreBuilder<?> mutation = serializedCopy(original);
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
            case 0:
                queryRescorer.setQueryWeight(queryRescorer.getQueryWeight() + 0.1f);
                break;
            case 1:
                queryRescorer.setRescoreQueryWeight(queryRescorer.getRescoreQueryWeight() + 0.1f);
                break;
            case 2:
                QueryRescoreMode other;
                do {
                    other = randomFrom(QueryRescoreMode.values());
                } while (other == queryRescorer.getScoreMode());
                queryRescorer.setScoreMode(other);
                break;
            case 3:
                // only increase the boost to make it a slightly different query
                queryRescorer.getRescoreQuery().boost(queryRescorer.getRescoreQuery().boost() + 0.1f);
                break;
            default:
                throw new IllegalStateException("unexpected random mutation in test");
            }
        }
        return mutation;
    }

    /**
     * create random shape that is put under test
     */
    public static QueryRescorerBuilder randomRescoreBuilder() {
        QueryBuilder queryBuilder = new MatchAllQueryBuilder().boost(randomFloat())
                .queryName(randomAsciiOfLength(20));
        org.elasticsearch.search.rescore.QueryRescorerBuilder rescorer = new
                org.elasticsearch.search.rescore.QueryRescorerBuilder(queryBuilder);
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

    private static RescoreBuilder<?> serializedCopy(RescoreBuilder<?> original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(original);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                return in.readNamedWriteable(RescoreBuilder.class);
            }
        }
    }

}
