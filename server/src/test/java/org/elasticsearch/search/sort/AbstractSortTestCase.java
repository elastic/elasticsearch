/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SortField;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public abstract class AbstractSortTestCase<T extends SortBuilder<T>> extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;

    protected static NamedWriteableRegistry namedWriteableRegistry;

    private static NamedXContentRegistry xContentRegistry;
    private static ScriptService scriptService;
    protected static String MOCK_SCRIPT_NAME = "dummy";

    @BeforeClass
    public static void init() {
        Settings baseSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Map<String, Function<Map<String, Object>, Object>> scripts = Collections.singletonMap(MOCK_SCRIPT_NAME, p -> null);
        ScriptEngine engine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        scriptService = new ScriptService(
            baseSettings,
            Collections.singletonMap(engine.getType(), engine),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );

        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
        xContentRegistry = null;
        scriptService = null;
    }

    /** Returns random sort that is put under test */
    protected abstract T createTestItem();

    /** Returns mutated version of original so the returned sort is different in terms of equals/hashcode */
    protected abstract T mutate(T original) throws IOException;

    /** Parse the sort from xContent. Just delegate to the SortBuilder's static fromXContent method. */
    protected abstract T fromXContent(XContentParser parser, String fieldName) throws IOException;

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
            try (XContentParser itemParser = createParser(shuffled)) {
                itemParser.nextToken();

                /*
                 * filter out name of sort, or field name to sort on for element fieldSort
                 */
                itemParser.nextToken();
                String elementName = itemParser.currentName();
                itemParser.nextToken();

                T parsedItem = fromXContent(itemParser, elementName);
                assertNotSame(testItem, parsedItem);
                assertEquals(testItem, parsedItem);
                assertEquals(testItem.hashCode(), parsedItem.hashCode());
                assertWarnings(testItem);
            }
        }
    }

    protected void assertWarnings(T testItem) {
        // assert potential warnings based on the test sort configuration. Do nothing by default, subtests can overwrite
    }

    /**
     * test that build() outputs a {@link SortField} that is similar to the one
     * we would get when parsing the xContent the sort builder is rendering out
     */
    public void testBuildSortField() throws IOException {
        SearchExecutionContext mockShardContext = createMockSearchExecutionContext();
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            T sortBuilder = createTestItem();
            SortFieldAndFormat sortField = Rewriteable.rewrite(sortBuilder, mockShardContext).build(mockShardContext);
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
    public void testEqualsAndHashcode() {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            checkEqualsAndHashCode(createTestItem(), this::copy, this::mutate);
        }
    }

    protected final SearchExecutionContext createMockSearchExecutionContext() {
        return createMockSearchExecutionContext(null);
    }

    protected final SearchExecutionContext createMockSearchExecutionContext(IndexSearcher searcher) {
        Index index = new Index(randomAlphaOfLengthBetween(1, 10), "_na_");
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(
            index,
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build()
        );
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(idxSettings, Mockito.mock(BitsetFilterCache.Listener.class));
        TriFunction<MappedFieldType, String, Supplier<SearchLookup>, IndexFieldData<?>> indexFieldDataLookup = (
            fieldType,
            fieldIndexName,
            searchLookup) -> {
            IndexFieldData.Builder builder = fieldType.fielddataBuilder(fieldIndexName, searchLookup);
            return builder.build(new IndexFieldDataCache.None(), null);
        };
        NestedLookup nestedLookup = NestedLookup.build(
            List.of(new NestedObjectMapper.Builder("path", Version.CURRENT).build(MapperBuilderContext.ROOT))
        );
        return new SearchExecutionContext(
            0,
            0,
            idxSettings,
            bitsetFilterCache,
            indexFieldDataLookup,
            null,
            null,
            null,
            scriptService,
            parserConfig(),
            namedWriteableRegistry,
            null,
            searcher,
            () -> randomNonNegativeLong(),
            null,
            null,
            () -> true,
            null,
            emptyMap()
        ) {

            @Override
            public MappedFieldType getFieldType(String name) {
                return provideMappedFieldType(name);
            }

            @Override
            public NestedLookup nestedLookup() {
                return nestedLookup;
            }
        };
    }

    /**
     * Return a field type. We use {@link NumberFieldMapper.NumberFieldType} by default since it is compatible with all sort modes
     * Tests that require other field types can override this.
     */
    protected MappedFieldType provideMappedFieldType(String name) {
        NumberFieldMapper.NumberFieldType doubleFieldType = new NumberFieldMapper.NumberFieldType(
            name,
            NumberFieldMapper.NumberType.DOUBLE
        );
        return doubleFieldType;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    protected static QueryBuilder randomNestedFilter() {
        int id = randomIntBetween(0, 2);
        return switch (id) {
            case 0 -> (new MatchAllQueryBuilder()).boost(randomFloat());
            case 1 -> (new IdsQueryBuilder()).boost(randomFloat());
            case 2 -> (new TermQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomDouble()).boost(randomFloat()));
            default -> throw new IllegalStateException("Only three query builders supported for testing sort");
        };
    }

    @SuppressWarnings("unchecked")
    private T copy(T original) throws IOException {
        /* The cast below is required to make Java 9 happy. Java 8 infers the T in copyWriterable to be the same as AbstractSortTestCase's
         * T but Java 9 infers it to be SortBuilder. */
        return (T) copyWriteable(
            original,
            namedWriteableRegistry,
            namedWriteableRegistry.getReader(SortBuilder.class, original.getWriteableName())
        );
    }
}
