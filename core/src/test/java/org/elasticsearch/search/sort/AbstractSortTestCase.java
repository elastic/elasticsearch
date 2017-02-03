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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper.Nested;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptServiceTests.TestEngineService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.script.ScriptType;
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

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public abstract class AbstractSortTestCase<T extends SortBuilder<T>> extends ESTestCase {
    private static final int NUMBER_OF_TESTBUILDERS = 20;

    protected static NamedWriteableRegistry namedWriteableRegistry;

    private static NamedXContentRegistry xContentRegistry;
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
            public CompiledScript compile(Script script, ScriptContext scriptContext) {
                return new CompiledScript(ScriptType.INLINE, "mockName", "test", script);
            }
        };

        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
        xContentRegistry = null;
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
            XContentParser itemParser = createParser(shuffled);
            itemParser.nextToken();

            /*
             * filter out name of sort, or field name to sort on for element fieldSort
             */
            itemParser.nextToken();
            String elementName = itemParser.currentName();
            itemParser.nextToken();

            QueryParseContext context = new QueryParseContext(itemParser);
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
            T deserializedsort = copy(testsort);
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
            checkEqualsAndHashCode(createTestItem(), this::copy, this::mutate);
        }
    }

    protected QueryShardContext createMockShardContext() {
        Index index = new Index(randomAsciiOfLengthBetween(1, 10), "_na_");
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(index,
            Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build());
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
        long nowInMillis = randomNonNegativeLong();
        return new QueryShardContext(0, idxSettings, bitsetFilterCache, ifds, null, null, scriptService,
                xContentRegistry(), null, null, () -> nowInMillis) {
            @Override
            public MappedFieldType fieldMapper(String name) {
                return provideMappedFieldType(name);
            }

            @Override
            public ObjectMapper getObjectMapper(String name) {
                BuilderContext context = new BuilderContext(this.getIndexSettings().getSettings(), new ContentPath());
                return new ObjectMapper.Builder<>(name).nested(Nested.newNested(false, false)).build(context);
            }
        };
    }

    /**
     * Return a field type. We use {@link NumberFieldMapper.NumberFieldType} by default since it is compatible with all sort modes
     * Tests that require other field type than double can override this.
     */
    protected MappedFieldType provideMappedFieldType(String name) {
        NumberFieldMapper.NumberFieldType doubleFieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
        doubleFieldType.setName(name);
        doubleFieldType.setHasDocValues(true);
        return doubleFieldType;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
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
    private T copy(T original) throws IOException {
        /* The cast below is required to make Java 9 happy. Java 8 infers the T in copyWriterable to be the same as AbstractSortTestCase's
         * T but Java 9 infers it to be SortBuilder. */
        return (T) copyWriteable(original, namedWriteableRegistry,
                namedWriteableRegistry.getReader(SortBuilder.class, original.getWriteableName()));
    }
}
