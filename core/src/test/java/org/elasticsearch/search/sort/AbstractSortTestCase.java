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

package org.elasticsearch.search.sort;

import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.core.LegacyDoubleFieldMapper.DoubleFieldType;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper.Nested;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptServiceTests.TestEngineService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public abstract class AbstractSortTestCase<T extends SortBuilder<T>> extends ESTestCase {

    protected static NamedWriteableRegistry namedWriteableRegistry;

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    static IndicesQueriesRegistry indicesQueriesRegistry;
    private static ScriptService scriptService;

    @BeforeClass
    public static void init() throws IOException {
        Path genericConfigFolder = createTempDir();
        Settings baseSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(Environment.PATH_CONF_SETTING.getKey(), genericConfigFolder)
                .build();
        Environment environment = new Environment(baseSettings);
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(Collections.emptyList());
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Collections.singletonList(new TestEngineService()));
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        scriptService = new ScriptService(baseSettings, environment,
                new ResourceWatcherService(baseSettings, null), scriptEngineRegistry, scriptContextRegistry, scriptSettings) {
            @Override
            public CompiledScript compile(Script script, ScriptContext scriptContext, Map<String, String> params, ClusterState state) {
                return new CompiledScript(ScriptType.INLINE, "mockName", "test", script);
            }
        };

        namedWriteableRegistry = new NamedWriteableRegistry();
        indicesQueriesRegistry = new SearchModule(Settings.EMPTY, namedWriteableRegistry).getQueryParserRegistry();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
        indicesQueriesRegistry = null;
    }

    /** Returns random sort that is put under test */
    protected abstract T createTestItem();

    /** Returns mutated version of original so the returned sort is different in terms of equals/hashcode */
    protected abstract T mutate(T original) throws IOException;

    /** Parse the sort from xContent. Just delegate to the SortBuilder's static fromXContent method. */
    protected abstract T fromXContent(QueryParseContext context, String fieldName) throws IOException;

    /**
     * Test that creates new sort from a random test sort and checks both for equality
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            T testItem = createTestItem();

            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            testItem.toXContent(builder, ToXContent.EMPTY_PARAMS);
            XContentBuilder shuffled = shuffleXContent(builder);
            XContentParser itemParser = XContentHelper.createParser(shuffled.bytes());
            itemParser.nextToken();

            /*
             * filter out name of sort, or field name to sort on for element fieldSort
             */
            itemParser.nextToken();
            String elementName = itemParser.currentName();
            itemParser.nextToken();

            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, itemParser, ParseFieldMatcher.STRICT);
            T parsedItem = fromXContent(context, elementName);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    /**
     * test that build() outputs a {@link SortField} that is similar to the one
     * we would get when parsing the xContent the sort builder is rendering out
     */
    public void testBuildSortField() throws IOException {
        QueryShardContext mockShardContext = createMockShardContext();
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            T sortBuilder = createTestItem();
            SortFieldAndFormat sortField = sortBuilder.build(mockShardContext);
            sortFieldAssertions(sortBuilder, sortField.field, sortField.format);
        }
    }

    protected abstract void sortFieldAssertions(T builder, SortField sortField, DocValueFormat format) throws IOException;

    /**
     * Test serialization and deserialization of the test sort.
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            T testsort = createTestItem();
            T deserializedsort = copyItem(testsort);
            assertEquals(testsort, deserializedsort);
            assertEquals(testsort.hashCode(), deserializedsort.hashCode());
            assertNotSame(testsort, deserializedsort);
        }
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            T firstsort = createTestItem();
            assertFalse("sort is equal to null", firstsort.equals(null));
            assertFalse("sort is equal to incompatible type", firstsort.equals(""));
            assertTrue("sort is not equal to self", firstsort.equals(firstsort));
            assertThat("same sort's hashcode returns different values if called multiple times", firstsort.hashCode(),
                    equalTo(firstsort.hashCode()));
            assertThat("different sorts should not be equal", mutate(firstsort), not(equalTo(firstsort)));
            assertThat("different sorts should have different hashcode", mutate(firstsort).hashCode(), not(equalTo(firstsort.hashCode())));

            T secondsort = copyItem(firstsort);
            assertTrue("sort is not equal to self", secondsort.equals(secondsort));
            assertTrue("sort is not equal to its copy", firstsort.equals(secondsort));
            assertTrue("equals is not symmetric", secondsort.equals(firstsort));
            assertThat("sort copy's hashcode is different from original hashcode", secondsort.hashCode(), equalTo(firstsort.hashCode()));

            T thirdsort = copyItem(secondsort);
            assertTrue("sort is not equal to self", thirdsort.equals(thirdsort));
            assertTrue("sort is not equal to its copy", secondsort.equals(thirdsort));
            assertThat("sort copy's hashcode is different from original hashcode", secondsort.hashCode(), equalTo(thirdsort.hashCode()));
            assertTrue("equals is not transitive", firstsort.equals(thirdsort));
            assertThat("sort copy's hashcode is different from original hashcode", firstsort.hashCode(), equalTo(thirdsort.hashCode()));
            assertTrue("equals is not symmetric", thirdsort.equals(secondsort));
            assertTrue("equals is not symmetric", thirdsort.equals(firstsort));
        }
    }

    protected QueryShardContext createMockShardContext() {
        Index index = new Index(randomAsciiOfLengthBetween(1, 10), "_na_");
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(index, Settings.EMPTY);
        IndicesFieldDataCache cache = new IndicesFieldDataCache(Settings.EMPTY, null);
        IndexFieldDataService ifds = new IndexFieldDataService(IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
                cache, null, null);
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(idxSettings, new BitsetFilterCache.Listener() {

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {
            }

            @Override
            public void onCache(ShardId shardId, Accountable accountable) {
            }
        });
        return new QueryShardContext(idxSettings, bitsetFilterCache, ifds, null, null, scriptService,
                indicesQueriesRegistry, null, null, null) {
            @Override
            public MappedFieldType fieldMapper(String name) {
                return provideMappedFieldType(name);
            }

            @Override
            public ObjectMapper getObjectMapper(String name) {
                BuilderContext context = new BuilderContext(Settings.EMPTY, new ContentPath());
                return new ObjectMapper.Builder<>(name).nested(Nested.newNested(false, false)).build(context);
            }
        };
    }

    /**
     * Return a field type. We use {@link DoubleFieldType} by default since it is compatible with all sort modes
     * Tests that require other field type than double can override this.
     */
    protected MappedFieldType provideMappedFieldType(String name) {
        DoubleFieldType doubleFieldType = new DoubleFieldType();
        doubleFieldType.setName(name);
        doubleFieldType.setHasDocValues(true);
        return doubleFieldType;
    }

    protected static QueryBuilder randomNestedFilter() {
        int id = randomIntBetween(0, 2);
        switch(id) {
            case 0: return (new MatchAllQueryBuilder()).boost(randomFloat());
            case 1: return (new IdsQueryBuilder()).boost(randomFloat());
            case 2: return (new TermQueryBuilder(
                    randomAsciiOfLengthBetween(1, 10),
                    randomDouble()).boost(randomFloat()));
            default: throw new IllegalStateException("Only three query builders supported for testing sort");
        }
    }

    @SuppressWarnings("unchecked")
    private T copyItem(T original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                return (T) namedWriteableRegistry.getReader(SortBuilder.class, original.getWriteableName()).read(in);
            }
        }
    }
}
