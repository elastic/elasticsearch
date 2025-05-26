/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.AnyOperatorTestCase;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * These tests are partial duplicates of the tests in <code>ValuesSourceReaderOperatorTests</code>, and focus on testing the behaviour
 * of the <code>ValuesSourceReaderOperator</code>, but with a few key differences:
 * <ul>
 *     <li>Multiple indexes and index mappings are defined and tested</li>
 *     <li>
 *         Most primitive types also include a field with prefix 'str_' which is stored and mapped as a string,
 *         but expected to be extracted and converted directly to the primitive type.
 *         For example: <code>"str_long": "1"</code> should be read directly into a field named "str_long" of type "long" and value 1.
 *         This tests the ability of the <code>BlockLoader.convert(Block)</code> method to convert a string to a primitive type.
 *     </li>
 *     <li>
 *         Each index has a few additional custom fields that are stored as specific types, but should be converted to strings by the
 *         <code>BlockLoader.convert(Block)</code> method. These fields are:
 *         <ul>
 *             <li>ip: stored as an IP type, but should be converted to a string</li>
 *             <li>duration: stored as a long type, but should be converted to a string</li>
 *         </ul>
 *         One index stores them as IP and long types, and the other as keyword types, so we test the behaviour of the
 *         'union types' capabilities of the <code>ValuesSourceReaderOperator</code> class.
 *     </li>
 * </ul>
 * Since this test does not have access to the type conversion code in the ESQL module, we have mocks for that behaviour
 * in the inner classes <code>TestTypeConvertingBlockLoader</code> and <code>TestBlockConverter</code>.
 */
@SuppressWarnings("resource")
public class ValueSourceReaderTypeConversionTests extends AnyOperatorTestCase {
    private static final String[] PREFIX = new String[] { "a", "b", "c" };
    private static final Map<String, TestIndexMappingConfig> INDICES = new LinkedHashMap<>();
    static {
        addIndex(
            Map.of(
                "ip",
                new TestFieldType<>("ip", IP, d -> "192.169.0." + d % 256, Checks::unionIPsAsStrings),
                "duration",
                new TestFieldType<>("duration", DataType.LONG, d -> (long) d, Checks::unionDurationsAsStrings)
            )
        );
        addIndex(
            Map.of(
                "ip",
                new TestFieldType<>("ip", DataType.KEYWORD, d -> "192.169.0." + d % 256, Checks::unionIPsAsStrings),
                "duration",
                new TestFieldType<>("duration", DataType.KEYWORD, d -> Integer.toString(d), Checks::unionDurationsAsStrings)
            )
        );
    }

    static void addIndex(Map<String, TestFieldType<?>> fieldTypes) {
        String indexKey = "index" + (INDICES.size() + 1);
        INDICES.put(indexKey, new TestIndexMappingConfig(indexKey, INDICES.size(), fieldTypes));
    }

    private record TestIndexMappingConfig(String indexName, int shardIdx, Map<String, TestFieldType<?>> fieldTypes) {}

    private record TestFieldType<T>(String name, DataType dataType, Function<Integer, T> valueGenerator, CheckResults checkResults) {}

    private final Map<String, Directory> directories = new HashMap<>();
    private final Map<String, MapperService> mapperServices = new HashMap<>();
    private final Map<String, IndexReader> readers = new HashMap<>();
    private static final Map<String, Map<Integer, String>> keyToTags = new HashMap<>();

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(readers.values());
        IOUtils.close(directories.values());
    }

    private Directory directory(String indexKey) {
        return directories.computeIfAbsent(indexKey, k -> newDirectory());
    }

    private MapperService mapperService(String indexKey) {
        return mapperServices.get(indexKey);
    }

    private List<ValuesSourceReaderOperator.ShardContext> initShardContexts() {
        return INDICES.keySet()
            .stream()
            .map(index -> new ValuesSourceReaderOperator.ShardContext(reader(index), () -> SourceLoader.FROM_STORED_SOURCE, 0.2))
            .toList();
    }

    private IndexReader reader(String indexKey) {
        if (readers.get(indexKey) == null) {
            try {
                initIndex(indexKey, 100, 10);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return readers.get(indexKey);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return factory(initShardContexts(), mapperService("index1").fieldType("long"), ElementType.LONG);
    }

    public static Operator.OperatorFactory factory(
        List<ValuesSourceReaderOperator.ShardContext> shardContexts,
        MappedFieldType ft,
        ElementType elementType
    ) {
        return factory(shardContexts, ft.name(), elementType, ft.blockLoader(blContext()));
    }

    private static Operator.OperatorFactory factory(
        List<ValuesSourceReaderOperator.ShardContext> shardContexts,
        String name,
        ElementType elementType,
        BlockLoader loader
    ) {
        return new ValuesSourceReaderOperator.Factory(List.of(new ValuesSourceReaderOperator.FieldInfo(name, elementType, shardIdx -> {
            if (shardIdx < 0 || shardIdx >= INDICES.size()) {
                fail("unexpected shardIdx [" + shardIdx + "]");
            }
            return loader;
        })), shardContexts, 0);
    }

    protected SourceOperator simpleInput(DriverContext context, int size) {
        return simpleInput(context, size, commitEvery(size), randomPageSize());
    }

    private int commitEvery(int numDocs) {
        return Math.max(1, (int) Math.ceil((double) numDocs / 10));
    }

    private SourceOperator simpleInput(DriverContext context, int size, int commitEvery, int pageSize) {
        List<LuceneSourceOperatorTests.MockShardContext> shardContexts = new ArrayList<>();
        try {
            for (String indexKey : INDICES.keySet()) {
                initIndex(indexKey, size, commitEvery);
                shardContexts.add(new LuceneSourceOperatorTests.MockShardContext(reader(indexKey), INDICES.get(indexKey).shardIdx));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        var luceneFactory = new LuceneSourceOperator.Factory(
            shardContexts,
            ctx -> new MatchAllDocsQuery(),
            DataPartitioning.SHARD,
            1,// randomIntBetween(1, 10),
            pageSize,
            LuceneOperator.NO_LIMIT,
            false // no scoring
        );
        return luceneFactory.get(context);
    }

    private void initMapping(String indexKey) throws IOException {
        TestIndexMappingConfig indexMappingConfig = INDICES.get(indexKey);
        mapperServices.put(indexKey, new MapperServiceTestCase() {
        }.createMapperService(MapperServiceTestCase.mapping(b -> {
            fieldExamples(b, "key", "integer"); // unique key per-index to use for looking up test values to compare to
            fieldExamples(b, "indexKey", "keyword");  // index name (can be used to choose index-specific test values)
            fieldExamples(b, "int", "integer");
            fieldExamples(b, "short", "short");
            fieldExamples(b, "byte", "byte");
            fieldExamples(b, "long", "long");
            fieldExamples(b, "double", "double");
            fieldExamples(b, "kwd", "keyword");
            b.startObject("stored_kwd").field("type", "keyword").field("store", true).endObject();
            b.startObject("mv_stored_kwd").field("type", "keyword").field("store", true).endObject();

            simpleField(b, "missing_text", "text");

            for (Map.Entry<String, TestFieldType<?>> entry : indexMappingConfig.fieldTypes.entrySet()) {
                String fieldName = entry.getKey();
                TestFieldType<?> fieldType = entry.getValue();
                simpleField(b, fieldName, fieldType.dataType.typeName());
            }
        })));
    }

    private void initIndex(String indexKey, int size, int commitEvery) throws IOException {
        initMapping(indexKey);
        readers.put(indexKey, initIndex(indexKey, directory(indexKey), size, commitEvery));
    }

    private IndexReader initIndex(String indexKey, Directory directory, int size, int commitEvery) throws IOException {
        keyToTags.computeIfAbsent(indexKey, k -> new HashMap<>()).clear();
        TestIndexMappingConfig indexMappingConfig = INDICES.get(indexKey);
        try (
            IndexWriter writer = new IndexWriter(
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            )
        ) {
            for (int d = 0; d < size; d++) {
                XContentBuilder source = JsonXContent.contentBuilder();
                source.startObject();
                source.field("key", d);  // documents in this index have a unique key, from which most other values can be derived
                source.field("indexKey", indexKey);  // all documents in this index have the same indexKey

                source.field("long", d);
                source.field("str_long", Long.toString(d));
                source.startArray("mv_long");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(-1_000L * d + v);
                }
                source.endArray();
                source.field("source_long", (long) d);
                source.startArray("mv_source_long");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(-1_000L * d + v);
                }
                source.endArray();

                source.field("int", d);
                source.field("str_int", Integer.toString(d));
                source.startArray("mv_int");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(1_000 * d + v);
                }
                source.endArray();
                source.field("source_int", d);
                source.startArray("mv_source_int");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(1_000 * d + v);
                }
                source.endArray();

                source.field("short", (short) d);
                source.field("str_short", Short.toString((short) d));
                source.startArray("mv_short");
                for (int v = 0; v <= d % 3; v++) {
                    source.value((short) (2_000 * d + v));
                }
                source.endArray();
                source.field("source_short", (short) d);
                source.startArray("mv_source_short");
                for (int v = 0; v <= d % 3; v++) {
                    source.value((short) (2_000 * d + v));
                }
                source.endArray();

                source.field("byte", (byte) d);
                source.field("str_byte", Byte.toString((byte) d));
                source.startArray("mv_byte");
                for (int v = 0; v <= d % 3; v++) {
                    source.value((byte) (3_000 * d + v));
                }
                source.endArray();
                source.field("source_byte", (byte) d);
                source.startArray("mv_source_byte");
                for (int v = 0; v <= d % 3; v++) {
                    source.value((byte) (3_000 * d + v));
                }
                source.endArray();

                source.field("double", d / 123_456d);
                source.field("str_double", Double.toString(d / 123_456d));
                source.startArray("mv_double");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(d / 123_456d + v);
                }
                source.endArray();
                source.field("source_double", d / 123_456d);
                source.startArray("mv_source_double");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(d / 123_456d + v);
                }
                source.endArray();

                String tag = keyToTags.get(indexKey).computeIfAbsent(d, k -> "tag-" + randomIntBetween(1, 5));
                source.field("kwd", tag);
                source.field("str_kwd", tag);
                source.startArray("mv_kwd");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(PREFIX[v] + d);
                }
                source.endArray();
                source.field("stored_kwd", Integer.toString(d));
                source.startArray("mv_stored_kwd");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(PREFIX[v] + d);
                }
                source.endArray();
                source.field("source_kwd", Integer.toString(d));
                source.startArray("mv_source_kwd");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(PREFIX[v] + d);
                }
                source.endArray();

                source.field("text", Integer.toString(d));
                source.startArray("mv_text");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(PREFIX[v] + d);
                }
                source.endArray();

                for (Map.Entry<String, TestFieldType<?>> entry : indexMappingConfig.fieldTypes.entrySet()) {
                    String fieldName = entry.getKey();
                    TestFieldType<?> fieldType = entry.getValue();
                    source.field(fieldName, fieldType.valueGenerator.apply(d));
                }

                source.endObject();

                ParsedDocument doc = mapperService(indexKey).documentParser()
                    .parseDocument(
                        new SourceToParse("id" + d, BytesReference.bytes(source), XContentType.JSON),
                        mapperService(indexKey).mappingLookup()
                    );
                writer.addDocuments(doc.docs());

                if (d % commitEvery == commitEvery - 1) {
                    writer.commit();
                }
            }
        }
        return DirectoryReader.open(directory);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("ValuesSourceReaderOperator[fields = [long]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    public void testLoadAll() {
        DriverContext driverContext = driverContext();
        loadSimpleAndAssert(
            driverContext,
            CannedSourceOperator.collectPages(simpleInput(driverContext, between(100, 5000))),
            Block.MvOrdering.SORTED_ASCENDING,
            Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
        );
    }

    public void testLoadAllInOnePage() {
        DriverContext driverContext = driverContext();
        loadSimpleAndAssert(
            driverContext,
            List.of(CannedSourceOperator.mergePages(CannedSourceOperator.collectPages(simpleInput(driverContext, between(100, 5000))))),
            Block.MvOrdering.UNORDERED,
            Block.MvOrdering.UNORDERED
        );
    }

    public void testManySingleDocPages() {
        String indexKey = "index1";
        DriverContext driverContext = driverContext();
        int numDocs = between(10, 100);
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext, numDocs, between(1, numDocs), 1));
        Randomness.shuffle(input);
        List<ValuesSourceReaderOperator.ShardContext> shardContexts = initShardContexts();
        List<Operator> operators = new ArrayList<>();
        Checks checks = new Checks(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING, Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
        FieldCase testCase = new FieldCase(
            new KeywordFieldMapper.KeywordFieldType("kwd"),
            ElementType.BYTES_REF,
            checks::tags,
            StatusChecks::keywordsFromDocValues
        );
        // TODO: Add index2
        operators.add(
            new ValuesSourceReaderOperator.Factory(
                List.of(testCase.info, fieldInfo(mapperService(indexKey).fieldType("key"), ElementType.INT)),
                shardContexts,
                0
            ).get(driverContext)
        );
        List<Page> results = drive(operators, input.iterator(), driverContext);
        assertThat(results, hasSize(input.size()));
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(3));
            IntVector keys = page.<IntBlock>getBlock(2).asVector();
            for (int p = 0; p < page.getPositionCount(); p++) {
                int key = keys.getInt(p);
                testCase.checkResults.check(page.getBlock(1), p, key, indexKey);
            }
        }
    }

    public void testEmpty() {
        DriverContext driverContext = driverContext();
        loadSimpleAndAssert(
            driverContext,
            CannedSourceOperator.collectPages(simpleInput(driverContext, 0)),
            Block.MvOrdering.UNORDERED,
            Block.MvOrdering.UNORDERED
        );
    }

    public void testLoadAllInOnePageShuffled() {
        DriverContext driverContext = driverContext();
        Page source = CannedSourceOperator.mergePages(CannedSourceOperator.collectPages(simpleInput(driverContext, between(100, 5000))));
        List<Integer> shuffleList = new ArrayList<>();
        IntStream.range(0, source.getPositionCount()).forEach(shuffleList::add);
        Randomness.shuffle(shuffleList);
        int[] shuffleArray = shuffleList.stream().mapToInt(Integer::intValue).toArray();
        Block[] shuffledBlocks = new Block[source.getBlockCount()];
        for (int b = 0; b < shuffledBlocks.length; b++) {
            shuffledBlocks[b] = source.getBlock(b).filter(shuffleArray);
        }
        source = new Page(shuffledBlocks);
        loadSimpleAndAssert(driverContext, List.of(source), Block.MvOrdering.UNORDERED, Block.MvOrdering.UNORDERED);
    }

    private static ValuesSourceReaderOperator.FieldInfo fieldInfo(MappedFieldType ft, ElementType elementType) {
        return new ValuesSourceReaderOperator.FieldInfo(ft.name(), elementType, shardIdx -> getBlockLoaderFor(shardIdx, ft, null));
    }

    private static ValuesSourceReaderOperator.FieldInfo fieldInfo(MappedFieldType ft, MappedFieldType ftX, ElementType elementType) {
        return new ValuesSourceReaderOperator.FieldInfo(ft.name(), elementType, shardIdx -> getBlockLoaderFor(shardIdx, ft, ftX));
    }

    private ValuesSourceReaderOperator.FieldInfo fieldInfo(String fieldName, ElementType elementType, DataType toType) {
        return new ValuesSourceReaderOperator.FieldInfo(fieldName, elementType, shardIdx -> getBlockLoaderFor(shardIdx, fieldName, toType));
    }

    private static MappedFieldType.BlockLoaderContext blContext() {
        return new MappedFieldType.BlockLoaderContext() {
            @Override
            public String indexName() {
                return "test_index";
            }

            @Override
            public IndexSettings indexSettings() {
                var imd = IndexMetadata.builder("test_index")
                    .settings(ValueSourceReaderTypeConversionTests.indexSettings(IndexVersion.current(), 1, 1).put(Settings.EMPTY))
                    .build();
                return new IndexSettings(imd, Settings.EMPTY);
            }

            @Override
            public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                return MappedFieldType.FieldExtractPreference.NONE;
            }

            @Override
            public SearchLookup lookup() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<String> sourcePaths(String name) {
                return Set.of(name);
            }

            @Override
            public String parentField(String field) {
                return null;
            }

            @Override
            public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
                return FieldNamesFieldMapper.FieldNamesFieldType.get(true);
            }
        };
    }

    private void loadSimpleAndAssert(
        DriverContext driverContext,
        List<Page> input,
        Block.MvOrdering booleanAndNumericalDocValuesMvOrdering,
        Block.MvOrdering bytesRefDocValuesMvOrdering
    ) {
        List<FieldCase> cases = infoAndChecksForEachType(booleanAndNumericalDocValuesMvOrdering, bytesRefDocValuesMvOrdering);
        List<ValuesSourceReaderOperator.ShardContext> shardContexts = initShardContexts();
        List<Operator> operators = new ArrayList<>();
        operators.add(
            new ValuesSourceReaderOperator.Factory(
                List.of(
                    fieldInfo(mapperService("index1").fieldType("key"), ElementType.INT),
                    fieldInfo(mapperService("index1").fieldType("indexKey"), ElementType.BYTES_REF)
                ),
                shardContexts,
                0
            ).get(driverContext)
        );
        List<FieldCase> tests = new ArrayList<>();
        while (cases.isEmpty() == false) {
            List<FieldCase> b = randomNonEmptySubsetOf(cases);
            cases.removeAll(b);
            tests.addAll(b);
            operators.add(
                new ValuesSourceReaderOperator.Factory(b.stream().map(i -> i.info).toList(), shardContexts, 0).get(driverContext)
            );
        }
        List<Page> results = drive(operators, input.iterator(), driverContext);
        assertThat(results, hasSize(input.size()));
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(tests.size() + 3 /* one for doc, one for keys and one for indexKey */));
            IntVector keys = page.<IntBlock>getBlock(1).asVector();
            BytesRefVector indexKeys = page.<BytesRefBlock>getBlock(2).asVector();
            for (int p = 0; p < page.getPositionCount(); p++) {
                int key = keys.getInt(p);
                String indexKey = indexKeys.getBytesRef(p, new BytesRef()).utf8ToString();
                for (int i = 0; i < tests.size(); i++) {
                    try {
                        tests.get(i).checkResults.check(page.getBlock(3 + i), p, key, indexKey);
                    } catch (AssertionError e) {
                        throw new AssertionError("error checking " + tests.get(i).info.name() + "[" + p + "]: " + e.getMessage(), e);
                    }
                }
            }
        }
        for (Operator op : operators) {
            assertThat(((ValuesSourceReaderOperator) op).status().pagesProcessed(), equalTo(input.size()));
        }
        assertDriverContext(driverContext);
    }

    interface CheckResults {
        void check(Block block, int position, int key, String indexKey);
    }

    interface CheckReaders {
        void check(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readersBuilt);
    }

    interface CheckReadersWithName {
        void check(String name, boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readersBuilt);
    }

    record FieldCase(ValuesSourceReaderOperator.FieldInfo info, CheckResults checkResults, CheckReadersWithName checkReaders) {
        FieldCase(MappedFieldType ft, ElementType elementType, CheckResults checkResults, CheckReadersWithName checkReaders) {
            this(fieldInfo(ft, elementType), checkResults, checkReaders);
        }

        FieldCase(
            MappedFieldType ft,
            MappedFieldType ftX,
            ElementType elementType,
            CheckResults checkResults,
            CheckReadersWithName checkReaders
        ) {
            this(fieldInfo(ft, ftX, elementType), checkResults, checkReaders);
        }

        FieldCase(MappedFieldType ft, ElementType elementType, CheckResults checkResults, CheckReaders checkReaders) {
            this(
                ft,
                elementType,
                checkResults,
                (name, forcedRowByRow, pageCount, segmentCount, readersBuilt) -> checkReaders.check(
                    forcedRowByRow,
                    pageCount,
                    segmentCount,
                    readersBuilt
                )
            );
        }
    }

    /**
     * Asserts that {@link ValuesSourceReaderOperator#status} claims that only
     * the expected readers are built after loading singleton pages.
     */
    public void testLoadAllStatus() {
        testLoadAllStatus(false);
    }

    /**
     * Asserts that {@link ValuesSourceReaderOperator#status} claims that only
     * the expected readers are built after loading non-singleton pages.
     */
    public void testLoadAllStatusAllInOnePage() {
        testLoadAllStatus(true);
    }

    private void testLoadAllStatus(boolean allInOnePage) {
        DriverContext driverContext = driverContext();
        int numDocs = between(100, 5000);
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext, numDocs, commitEvery(numDocs), numDocs));
        assertThat(input, hasSize(20));
        List<ValuesSourceReaderOperator.ShardContext> shardContexts = initShardContexts();
        int totalSize = 0;
        for (var shardContext : shardContexts) {
            assertThat(shardContext.reader().leaves(), hasSize(10));
            totalSize += shardContext.reader().leaves().size();
        }
        // Build one operator for each field, so we get a unique map to assert on
        List<FieldCase> cases = infoAndChecksForEachType(
            Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING,
            Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
        );
        List<Operator> operators = cases.stream()
            .map(i -> new ValuesSourceReaderOperator.Factory(List.of(i.info), shardContexts, 0).get(driverContext))
            .toList();
        if (allInOnePage) {
            input = List.of(CannedSourceOperator.mergePages(input));
        }
        drive(operators, input.iterator(), driverContext);
        for (int i = 0; i < cases.size(); i++) {
            ValuesSourceReaderOperator.Status status = (ValuesSourceReaderOperator.Status) operators.get(i).status();
            assertThat(status.pagesProcessed(), equalTo(input.size()));
            FieldCase fc = cases.get(i);
            fc.checkReaders.check(fc.info.name(), allInOnePage, input.size(), totalSize, status.readersBuilt());
        }
    }

    private List<FieldCase> infoAndChecksForEachType(
        Block.MvOrdering booleanAndNumericalDocValuesMvOrdering,
        Block.MvOrdering bytesRefDocValuesMvOrdering
    ) {
        MapperService mapperService = mapperService("index1");  // almost fields have identical mapper service
        Checks checks = new Checks(booleanAndNumericalDocValuesMvOrdering, bytesRefDocValuesMvOrdering);
        List<FieldCase> r = new ArrayList<>();
        r.add(new FieldCase(mapperService.fieldType(IdFieldMapper.NAME), ElementType.BYTES_REF, checks::ids, StatusChecks::id));
        r.add(new FieldCase(TsidExtractingIdFieldMapper.INSTANCE.fieldType(), ElementType.BYTES_REF, checks::ids, StatusChecks::id));
        r.add(new FieldCase(mapperService.fieldType("long"), ElementType.LONG, checks::longs, StatusChecks::longsFromDocValues));
        r.add(
            new FieldCase(
                mapperService.fieldType("str_long"),
                mapperService.fieldType("long"),
                ElementType.LONG,
                checks::longs,
                StatusChecks::strFromDocValues
            )
        );
        r.add(
            new FieldCase(
                mapperService.fieldType("mv_long"),
                ElementType.LONG,
                checks::mvLongsFromDocValues,
                StatusChecks::mvLongsFromDocValues
            )
        );
        r.add(new FieldCase(mapperService.fieldType("missing_long"), ElementType.LONG, checks::constantNulls, StatusChecks::constantNulls));
        r.add(new FieldCase(mapperService.fieldType("source_long"), ElementType.LONG, checks::longs, StatusChecks::longsFromSource));
        r.add(
            new FieldCase(
                mapperService.fieldType("mv_source_long"),
                ElementType.LONG,
                checks::mvLongsUnordered,
                StatusChecks::mvLongsFromSource
            )
        );
        r.add(new FieldCase(mapperService.fieldType("int"), ElementType.INT, checks::ints, StatusChecks::intsFromDocValues));
        r.add(
            new FieldCase(
                mapperService.fieldType("str_int"),
                mapperService.fieldType("int"),
                ElementType.INT,
                checks::ints,
                StatusChecks::strFromDocValues
            )
        );
        r.add(
            new FieldCase(
                mapperService.fieldType("mv_int"),
                ElementType.INT,
                checks::mvIntsFromDocValues,
                StatusChecks::mvIntsFromDocValues
            )
        );
        r.add(new FieldCase(mapperService.fieldType("missing_int"), ElementType.INT, checks::constantNulls, StatusChecks::constantNulls));
        r.add(new FieldCase(mapperService.fieldType("source_int"), ElementType.INT, checks::ints, StatusChecks::intsFromSource));
        r.add(
            new FieldCase(
                mapperService.fieldType("mv_source_int"),
                ElementType.INT,
                checks::mvIntsUnordered,
                StatusChecks::mvIntsFromSource
            )
        );
        r.add(new FieldCase(mapperService.fieldType("short"), ElementType.INT, checks::shorts, StatusChecks::shortsFromDocValues));
        r.add(
            new FieldCase(
                mapperService.fieldType("str_short"),
                mapperService.fieldType("short"),
                ElementType.INT,
                checks::shorts,
                StatusChecks::strFromDocValues
            )
        );
        r.add(new FieldCase(mapperService.fieldType("mv_short"), ElementType.INT, checks::mvShorts, StatusChecks::mvShortsFromDocValues));
        r.add(new FieldCase(mapperService.fieldType("missing_short"), ElementType.INT, checks::constantNulls, StatusChecks::constantNulls));
        r.add(new FieldCase(mapperService.fieldType("byte"), ElementType.INT, checks::bytes, StatusChecks::bytesFromDocValues));
        // r.add(new FieldCase(mapperService.fieldType("str_byte"), ElementType.INT, checks::bytes, StatusChecks::bytesFromDocValues));
        r.add(new FieldCase(mapperService.fieldType("mv_byte"), ElementType.INT, checks::mvBytes, StatusChecks::mvBytesFromDocValues));
        r.add(new FieldCase(mapperService.fieldType("missing_byte"), ElementType.INT, checks::constantNulls, StatusChecks::constantNulls));
        r.add(new FieldCase(mapperService.fieldType("double"), ElementType.DOUBLE, checks::doubles, StatusChecks::doublesFromDocValues));
        r.add(
            new FieldCase(
                mapperService.fieldType("str_double"),
                mapperService.fieldType("double"),
                ElementType.DOUBLE,
                checks::doubles,
                StatusChecks::strFromDocValues
            )
        );
        r.add(
            new FieldCase(mapperService.fieldType("mv_double"), ElementType.DOUBLE, checks::mvDoubles, StatusChecks::mvDoublesFromDocValues)
        );
        r.add(
            new FieldCase(mapperService.fieldType("missing_double"), ElementType.DOUBLE, checks::constantNulls, StatusChecks::constantNulls)
        );
        r.add(new FieldCase(mapperService.fieldType("kwd"), ElementType.BYTES_REF, checks::tags, StatusChecks::keywordsFromDocValues));
        r.add(
            new FieldCase(
                mapperService.fieldType("mv_kwd"),
                ElementType.BYTES_REF,
                checks::mvStringsFromDocValues,
                StatusChecks::mvKeywordsFromDocValues
            )
        );
        r.add(
            new FieldCase(mapperService.fieldType("missing_kwd"), ElementType.BYTES_REF, checks::constantNulls, StatusChecks::constantNulls)
        );
        r.add(new FieldCase(storedKeywordField("stored_kwd"), ElementType.BYTES_REF, checks::strings, StatusChecks::keywordsFromStored));
        r.add(
            new FieldCase(
                storedKeywordField("mv_stored_kwd"),
                ElementType.BYTES_REF,
                checks::mvStringsUnordered,
                StatusChecks::mvKeywordsFromStored
            )
        );
        r.add(
            new FieldCase(mapperService.fieldType("source_kwd"), ElementType.BYTES_REF, checks::strings, StatusChecks::keywordsFromSource)
        );
        r.add(
            new FieldCase(
                mapperService.fieldType("mv_source_kwd"),
                ElementType.BYTES_REF,
                checks::mvStringsUnordered,
                StatusChecks::mvKeywordsFromSource
            )
        );
        r.add(
            new FieldCase(
                new ValuesSourceReaderOperator.FieldInfo(
                    "constant_bytes",
                    ElementType.BYTES_REF,
                    shardIdx -> BlockLoader.constantBytes(new BytesRef("foo"))
                ),
                checks::constantBytes,
                StatusChecks::constantBytes
            )
        );
        r.add(
            new FieldCase(
                new ValuesSourceReaderOperator.FieldInfo("null", ElementType.NULL, shardIdx -> BlockLoader.CONSTANT_NULLS),
                checks::constantNulls,
                StatusChecks::constantNulls
            )
        );

        // We only care about the field name at this point, so we can use any index mapper here
        TestIndexMappingConfig indexMappingConfig = INDICES.get("index1");
        for (TestFieldType<?> fieldType : indexMappingConfig.fieldTypes.values()) {
            r.add(
                new FieldCase(
                    fieldInfo(fieldType.name, ElementType.BYTES_REF, DataType.KEYWORD),
                    fieldType.checkResults,
                    StatusChecks::unionFromDocValues
                )
            );
        }
        Collections.shuffle(r, random());
        return r;
    }

    record Checks(Block.MvOrdering booleanAndNumericalDocValuesMvOrdering, Block.MvOrdering bytesRefDocValuesMvOrdering) {
        void longs(Block block, int position, int key, String indexKey) {
            LongVector longs = ((LongBlock) block).asVector();
            assertThat(longs.getLong(position), equalTo((long) key));
        }

        void ints(Block block, int position, int key, String indexKey) {
            IntVector ints = ((IntBlock) block).asVector();
            assertThat(ints.getInt(position), equalTo(key));
        }

        void shorts(Block block, int position, int key, String indexKey) {
            IntVector ints = ((IntBlock) block).asVector();
            assertThat(ints.getInt(position), equalTo((int) (short) key));
        }

        void bytes(Block block, int position, int key, String indexKey) {
            IntVector ints = ((IntBlock) block).asVector();
            assertThat(ints.getInt(position), equalTo((int) (byte) key));
        }

        void doubles(Block block, int position, int key, String indexKey) {
            DoubleVector doubles = ((DoubleBlock) block).asVector();
            assertThat(doubles.getDouble(position), equalTo(key / 123_456d));
        }

        void strings(Block block, int position, int key, String indexKey) {
            BytesRefVector keywords = ((BytesRefBlock) block).asVector();
            assertThat(keywords.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo(Integer.toString(key)));
        }

        static void unionIPsAsStrings(Block block, int position, int key, String indexKey) {
            BytesRefVector keywords = ((BytesRefBlock) block).asVector();
            BytesRef bytesRef = keywords.getBytesRef(position, new BytesRef());
            TestIndexMappingConfig mappingConfig = INDICES.get(indexKey);
            TestFieldType<?> fieldType = mappingConfig.fieldTypes.get("ip");
            String expected = fieldType.valueGenerator.apply(key).toString();
            // Conversion should already be done in FieldInfo!
            // BytesRef found = (fieldType.dataType.typeName().equals("ip")) ? new BytesRef(DocValueFormat.IP.format(bytesRef)) : bytesRef;
            assertThat(bytesRef.utf8ToString(), equalTo(expected));
        }

        static void unionDurationsAsStrings(Block block, int position, int key, String indexKey) {
            BytesRefVector keywords = ((BytesRefBlock) block).asVector();
            BytesRef bytesRef = keywords.getBytesRef(position, new BytesRef());
            TestIndexMappingConfig mappingConfig = INDICES.get(indexKey);
            TestFieldType<?> fieldType = mappingConfig.fieldTypes.get("duration");
            String expected = fieldType.valueGenerator.apply(key).toString();
            assertThat(bytesRef.utf8ToString(), equalTo(expected));
        }

        void tags(Block block, int position, int key, String indexKey) {
            BytesRefVector keywords = ((BytesRefBlock) block).asVector();
            Object[] validTags = INDICES.keySet().stream().map(keyToTags::get).map(t -> t.get(key)).toArray();
            assertThat(keywords.getBytesRef(position, new BytesRef()).utf8ToString(), oneOf(validTags));
        }

        void ids(Block block, int position, int key, String indexKey) {
            BytesRefVector ids = ((BytesRefBlock) block).asVector();
            assertThat(ids.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo("id" + key));
        }

        void constantBytes(Block block, int position, int key, String indexKey) {
            BytesRefVector keywords = ((BytesRefBlock) block).asVector();
            assertThat(keywords.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo("foo"));
        }

        void constantNulls(Block block, int position, int key, String indexKey) {
            assertTrue(block.areAllValuesNull());
            assertTrue(block.isNull(position));
        }

        void mvLongsFromDocValues(Block block, int position, int key, String indexKey) {
            mvLongs(block, position, key, booleanAndNumericalDocValuesMvOrdering);
        }

        void mvLongsUnordered(Block block, int position, int key, String indexKey) {
            mvLongs(block, position, key, Block.MvOrdering.UNORDERED);
        }

        private void mvLongs(Block block, int position, int key, Block.MvOrdering expectedMv) {
            LongBlock longs = (LongBlock) block;
            assertThat(longs.getValueCount(position), equalTo(key % 3 + 1));
            int offset = longs.getFirstValueIndex(position);
            for (int v = 0; v <= key % 3; v++) {
                assertThat(longs.getLong(offset + v), equalTo(-1_000L * key + v));
            }
            if (key % 3 > 0) {
                assertThat(longs.mvOrdering(), equalTo(expectedMv));
            }
        }

        void mvIntsFromDocValues(Block block, int position, int key, String indexKey) {
            mvInts(block, position, key, booleanAndNumericalDocValuesMvOrdering);
        }

        void mvIntsUnordered(Block block, int position, int key, String indexKey) {
            mvInts(block, position, key, Block.MvOrdering.UNORDERED);
        }

        private void mvInts(Block block, int position, int key, Block.MvOrdering expectedMv) {
            IntBlock ints = (IntBlock) block;
            assertThat(ints.getValueCount(position), equalTo(key % 3 + 1));
            int offset = ints.getFirstValueIndex(position);
            for (int v = 0; v <= key % 3; v++) {
                assertThat(ints.getInt(offset + v), equalTo(1_000 * key + v));
            }
            if (key % 3 > 0) {
                assertThat(ints.mvOrdering(), equalTo(expectedMv));
            }
        }

        void mvShorts(Block block, int position, int key, String indexKey) {
            IntBlock ints = (IntBlock) block;
            assertThat(ints.getValueCount(position), equalTo(key % 3 + 1));
            int offset = ints.getFirstValueIndex(position);
            for (int v = 0; v <= key % 3; v++) {
                assertThat(ints.getInt(offset + v), equalTo((int) (short) (2_000 * key + v)));
            }
            if (key % 3 > 0) {
                assertThat(ints.mvOrdering(), equalTo(booleanAndNumericalDocValuesMvOrdering));
            }
        }

        void mvBytes(Block block, int position, int key, String indexKey) {
            IntBlock ints = (IntBlock) block;
            assertThat(ints.getValueCount(position), equalTo(key % 3 + 1));
            int offset = ints.getFirstValueIndex(position);
            for (int v = 0; v <= key % 3; v++) {
                assertThat(ints.getInt(offset + v), equalTo((int) (byte) (3_000 * key + v)));
            }
            if (key % 3 > 0) {
                assertThat(ints.mvOrdering(), equalTo(booleanAndNumericalDocValuesMvOrdering));
            }
        }

        void mvDoubles(Block block, int position, int key, String indexKey) {
            DoubleBlock doubles = (DoubleBlock) block;
            int offset = doubles.getFirstValueIndex(position);
            for (int v = 0; v <= key % 3; v++) {
                assertThat(doubles.getDouble(offset + v), equalTo(key / 123_456d + v));
            }
            if (key % 3 > 0) {
                assertThat(doubles.mvOrdering(), equalTo(booleanAndNumericalDocValuesMvOrdering));
            }
        }

        void mvStringsFromDocValues(Block block, int position, int key, String indexKey) {
            mvStrings(block, position, key, bytesRefDocValuesMvOrdering);
        }

        void mvStringsUnordered(Block block, int position, int key, String indexKey) {
            mvStrings(block, position, key, Block.MvOrdering.UNORDERED);
        }

        void mvStrings(Block block, int position, int key, Block.MvOrdering expectedMv) {
            BytesRefBlock text = (BytesRefBlock) block;
            assertThat(text.getValueCount(position), equalTo(key % 3 + 1));
            int offset = text.getFirstValueIndex(position);
            for (int v = 0; v <= key % 3; v++) {
                assertThat(text.getBytesRef(offset + v, new BytesRef()).utf8ToString(), equalTo(PREFIX[v] + key));
            }
            if (key % 3 > 0) {
                assertThat(text.mvOrdering(), equalTo(expectedMv));
            }
        }
    }

    static class StatusChecks {

        static void strFromDocValues(String name, boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues(name, "Ordinals", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void longsFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("long", "Longs", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void longsFromSource(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("source_long", "Longs", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void intsFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("int", "Ints", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void intsFromSource(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("source_int", "Ints", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void shortsFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("short", "Ints", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void bytesFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("byte", "Ints", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void doublesFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("double", "Doubles", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void keywordsFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("kwd", "Ordinals", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void keywordsFromStored(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            stored("stored_kwd", "Bytes", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void keywordsFromSource(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("source_kwd", "Bytes", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void mvLongsFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_long", "Longs", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void mvLongsFromSource(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("mv_source_long", "Longs", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void mvIntsFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_int", "Ints", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void mvIntsFromSource(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("mv_source_int", "Ints", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void mvShortsFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_short", "Ints", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void mvBytesFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_byte", "Ints", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void mvDoublesFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_double", "Doubles", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void mvKeywordsFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_kwd", "Ordinals", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void mvKeywordsFromStored(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            stored("mv_stored_kwd", "Bytes", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void mvKeywordsFromSource(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("mv_source_kwd", "Bytes", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void unionFromDocValues(String name, boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            // TODO: develop a working check for this
            // docValues(name, "Ordinals", forcedRowByRow, pageCount, segmentCount, readers);
        }

        private static void docValues(
            String name,
            String type,
            boolean forcedRowByRow,
            int pageCount,
            int segmentCount,
            Map<?, ?> readers
        ) {
            if (forcedRowByRow) {
                assertMap(
                    "Expected segment count in " + readers + "\n",
                    readers,
                    matchesMap().entry(name + ":row_stride:BlockDocValuesReader.Singleton" + type, lessThanOrEqualTo(segmentCount))
                );
            } else {
                assertMap(
                    "Expected segment count in " + readers + "\n",
                    readers,
                    matchesMap().entry(name + ":column_at_a_time:BlockDocValuesReader.Singleton" + type, lessThanOrEqualTo(pageCount))
                );
            }
        }

        private static void mvDocValues(
            String name,
            String type,
            boolean forcedRowByRow,
            int pageCount,
            int segmentCount,
            Map<?, ?> readers
        ) {
            if (forcedRowByRow) {
                Integer singletons = (Integer) readers.remove(name + ":row_stride:BlockDocValuesReader.Singleton" + type);
                if (singletons != null) {
                    segmentCount -= singletons;
                }
                assertMap(readers, matchesMap().entry(name + ":row_stride:BlockDocValuesReader." + type, segmentCount));
            } else {
                Integer singletons = (Integer) readers.remove(name + ":column_at_a_time:BlockDocValuesReader.Singleton" + type);
                if (singletons != null) {
                    pageCount -= singletons;
                }
                assertMap(
                    readers,
                    matchesMap().entry(name + ":column_at_a_time:BlockDocValuesReader." + type, lessThanOrEqualTo(pageCount))
                );
            }
        }

        static void id(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            stored("_id", "Id", forcedRowByRow, pageCount, segmentCount, readers);
        }

        private static void source(String name, String type, boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            Matcher<Integer> count;
            if (forcedRowByRow) {
                count = equalTo(segmentCount);
            } else {
                count = lessThanOrEqualTo(pageCount);
                Integer columnAttempts = (Integer) readers.remove(name + ":column_at_a_time:null");
                assertThat(columnAttempts, not(nullValue()));
            }

            Integer sequentialCount = (Integer) readers.remove("stored_fields[requires_source:true, fields:0, sequential: true]");
            Integer nonSequentialCount = (Integer) readers.remove("stored_fields[requires_source:true, fields:0, sequential: false]");
            int totalReaders = (sequentialCount == null ? 0 : sequentialCount) + (nonSequentialCount == null ? 0 : nonSequentialCount);
            assertThat(totalReaders, count);

            assertMap(readers, matchesMap().entry(name + ":row_stride:BlockSourceReader." + type, count));
        }

        private static void stored(String name, String type, boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            Matcher<Integer> count;
            if (forcedRowByRow) {
                count = equalTo(segmentCount);
            } else {
                count = lessThanOrEqualTo(pageCount);
                Integer columnAttempts = (Integer) readers.remove(name + ":column_at_a_time:null");
                assertThat(columnAttempts, not(nullValue()));
            }

            Integer sequentialCount = (Integer) readers.remove("stored_fields[requires_source:false, fields:1, sequential: true]");
            Integer nonSequentialCount = (Integer) readers.remove("stored_fields[requires_source:false, fields:1, sequential: false]");
            int totalReaders = (sequentialCount == null ? 0 : sequentialCount) + (nonSequentialCount == null ? 0 : nonSequentialCount);
            assertThat(totalReaders, count);

            assertMap(readers, matchesMap().entry(name + ":row_stride:BlockStoredFieldsReader." + type, count));
        }

        static void constantBytes(String name, boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            if (forcedRowByRow) {
                assertMap(readers, matchesMap().entry(name + ":row_stride:constant[[66 6f 6f]]", segmentCount));
            } else {
                assertMap(readers, matchesMap().entry(name + ":column_at_a_time:constant[[66 6f 6f]]", lessThanOrEqualTo(pageCount)));
            }
        }

        static void constantNulls(String name, boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            if (forcedRowByRow) {
                assertMap(readers, matchesMap().entry(name + ":row_stride:constant_nulls", segmentCount));
            } else {
                assertMap(readers, matchesMap().entry(name + ":column_at_a_time:constant_nulls", lessThanOrEqualTo(pageCount)));
            }
        }
    }

    public void testWithNulls() throws IOException {
        String indexKey = "index1";
        mapperServices.put(indexKey, new MapperServiceTestCase() {
        }.createMapperService(MapperServiceTestCase.mapping(b -> {
            fieldExamples(b, "i", "integer");
            fieldExamples(b, "j", "long");
            fieldExamples(b, "d", "double");
        })));
        MappedFieldType intFt = mapperService(indexKey).fieldType("i");
        MappedFieldType longFt = mapperService(indexKey).fieldType("j");
        MappedFieldType doubleFt = mapperService(indexKey).fieldType("d");
        MappedFieldType kwFt = new KeywordFieldMapper.KeywordFieldType("kw");

        NumericDocValuesField intField = new NumericDocValuesField(intFt.name(), 0);
        NumericDocValuesField longField = new NumericDocValuesField(longFt.name(), 0);
        NumericDocValuesField doubleField = new DoubleDocValuesField(doubleFt.name(), 0);
        final int numDocs = between(100, 5000);
        try (RandomIndexWriter w = new RandomIndexWriter(random(), directory(indexKey))) {
            Document doc = new Document();
            for (int i = 0; i < numDocs; i++) {
                doc.clear();
                intField.setLongValue(i);
                doc.add(intField);
                if (i % 100 != 0) { // Do not set field for every 100 values
                    longField.setLongValue(i);
                    doc.add(longField);
                    doubleField.setDoubleValue(i);
                    doc.add(doubleField);
                    doc.add(new SortedDocValuesField(kwFt.name(), new BytesRef("kw=" + i)));
                }
                w.addDocument(doc);
            }
            w.commit();
            readers.put(indexKey, w.getReader());
        }
        LuceneSourceOperatorTests.MockShardContext shardContext = new LuceneSourceOperatorTests.MockShardContext(reader(indexKey), 0);
        DriverContext driverContext = driverContext();
        var luceneFactory = new LuceneSourceOperator.Factory(
            List.of(shardContext),
            ctx -> new MatchAllDocsQuery(),
            randomFrom(DataPartitioning.values()),
            randomIntBetween(1, 10),
            randomPageSize(),
            LuceneOperator.NO_LIMIT,
            false // no scoring
        );
        var vsShardContext = new ValuesSourceReaderOperator.ShardContext(reader(indexKey), () -> SourceLoader.FROM_STORED_SOURCE, 0.2);
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                luceneFactory.get(driverContext),
                List.of(
                    factory(List.of(vsShardContext), intFt, ElementType.INT).get(driverContext),
                    factory(List.of(vsShardContext), longFt, ElementType.LONG).get(driverContext),
                    factory(List.of(vsShardContext), doubleFt, ElementType.DOUBLE).get(driverContext),
                    factory(List.of(vsShardContext), kwFt, ElementType.BYTES_REF).get(driverContext)
                ),
                new PageConsumerOperator(page -> {
                    try {
                        logger.debug("New page: {}", page);
                        IntBlock intValuesBlock = page.getBlock(1);
                        LongBlock longValuesBlock = page.getBlock(2);
                        DoubleBlock doubleValuesBlock = page.getBlock(3);
                        BytesRefBlock keywordValuesBlock = page.getBlock(4);

                        for (int i = 0; i < page.getPositionCount(); i++) {
                            assertFalse(intValuesBlock.isNull(i));
                            long j = intValuesBlock.getInt(i);
                            // Every 100 documents we set fields to null
                            boolean fieldIsEmpty = j % 100 == 0;
                            assertEquals(fieldIsEmpty, longValuesBlock.isNull(i));
                            assertEquals(fieldIsEmpty, doubleValuesBlock.isNull(i));
                            assertEquals(fieldIsEmpty, keywordValuesBlock.isNull(i));
                        }
                    } finally {
                        page.releaseBlocks();
                    }
                })
            )
        ) {
            runDriver(driver);
        }
        assertDriverContext(driverContext);
    }

    private XContentBuilder fieldExamples(XContentBuilder builder, String name, String type) throws IOException {
        simpleField(builder, name, type);
        simpleField(builder, "str_" + name, "keyword");
        simpleField(builder, "mv_" + name, type);
        simpleField(builder, "missing_" + name, type);
        sourceField(builder, "source_" + name, type);
        return sourceField(builder, "mv_source_" + name, type);
    }

    private XContentBuilder simpleField(XContentBuilder builder, String name, String type) throws IOException {
        return builder.startObject(name).field("type", type).endObject();
    }

    private XContentBuilder sourceField(XContentBuilder builder, String name, String type) throws IOException {
        return builder.startObject(name).field("type", type).field("store", false).field("doc_values", false).endObject();
    }

    private KeywordFieldMapper.KeywordFieldType storedKeywordField(String name) {
        FieldType ft = new FieldType(KeywordFieldMapper.Defaults.FIELD_TYPE);
        ft.setDocValuesType(DocValuesType.NONE);
        ft.setStored(true);
        ft.freeze();
        return new KeywordFieldMapper.KeywordFieldType(
            name,
            ft,
            Lucene.KEYWORD_ANALYZER,
            Lucene.KEYWORD_ANALYZER,
            Lucene.KEYWORD_ANALYZER,
            new KeywordFieldMapper.Builder(name, IndexVersion.current()).docValues(false),
            true // TODO randomize - load from stored keyword fields if stored even in synthetic source
        );
    }

    @AwaitsFix(bugUrl = "Get working for multiple indices")
    public void testNullsShared() {
        DriverContext driverContext = driverContext();
        List<ValuesSourceReaderOperator.ShardContext> shardContexts = initShardContexts();
        int[] pages = new int[] { 0 };
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                simpleInput(driverContext, 10),
                List.of(
                    new ValuesSourceReaderOperator.Factory(
                        List.of(
                            new ValuesSourceReaderOperator.FieldInfo("null1", ElementType.NULL, shardIdx -> BlockLoader.CONSTANT_NULLS),
                            new ValuesSourceReaderOperator.FieldInfo("null2", ElementType.NULL, shardIdx -> BlockLoader.CONSTANT_NULLS)
                        ),
                        shardContexts,
                        0
                    ).get(driverContext)
                ),
                new PageConsumerOperator(page -> {
                    try {
                        assertThat(page.getBlockCount(), equalTo(3));
                        assertThat(page.getBlock(1).areAllValuesNull(), equalTo(true));
                        assertThat(page.getBlock(2).areAllValuesNull(), equalTo(true));
                        assertThat(page.getBlock(1), sameInstance(page.getBlock(2)));
                        pages[0]++;
                    } finally {
                        page.releaseBlocks();
                    }
                })
            )
        ) {
            runDriver(d);
        }
        assertThat(pages[0], greaterThan(0));
        assertDriverContext(driverContext);
    }

    public void testDescriptionOfMany() throws IOException {
        String indexKey = "index1";
        initIndex(indexKey, 1, 1);
        Block.MvOrdering ordering = randomFrom(Block.MvOrdering.values());
        List<FieldCase> cases = infoAndChecksForEachType(ordering, ordering);

        ValuesSourceReaderOperator.Factory factory = new ValuesSourceReaderOperator.Factory(
            cases.stream().map(c -> c.info).toList(),
            List.of(new ValuesSourceReaderOperator.ShardContext(reader(indexKey), () -> SourceLoader.FROM_STORED_SOURCE, 0.2)),
            0
        );
        assertThat(factory.describe(), equalTo("ValuesSourceReaderOperator[fields = [" + cases.size() + " fields]]"));
        try (Operator op = factory.get(driverContext())) {
            assertThat(op.toString(), equalTo("ValuesSourceReaderOperator[fields = [" + cases.size() + " fields]]"));
        }
    }

    public void testManyShards() throws IOException {
        String indexKey = "index1";
        initMapping(indexKey);
        int shardCount = between(2, 10);
        int size = between(100, 1000);
        Directory[] dirs = new Directory[shardCount];
        IndexReader[] readers = new IndexReader[shardCount];
        Closeable[] closeMe = new Closeable[shardCount * 2];
        Set<Integer> seenShards = new TreeSet<>();
        Map<Integer, Integer> keyCounts = new TreeMap<>();
        try {
            for (int d = 0; d < dirs.length; d++) {
                closeMe[d * 2 + 1] = dirs[d] = newDirectory();
                closeMe[d * 2] = readers[d] = initIndex(indexKey, dirs[d], size, between(10, size * 2));
            }
            List<ShardContext> contexts = new ArrayList<>();
            List<ValuesSourceReaderOperator.ShardContext> readerShardContexts = new ArrayList<>();
            for (int s = 0; s < shardCount; s++) {
                contexts.add(new LuceneSourceOperatorTests.MockShardContext(readers[s], s));
                readerShardContexts.add(
                    new ValuesSourceReaderOperator.ShardContext(readers[s], () -> SourceLoader.FROM_STORED_SOURCE, 0.2)
                );
            }
            var luceneFactory = new LuceneSourceOperator.Factory(
                contexts,
                ctx -> new MatchAllDocsQuery(),
                DataPartitioning.SHARD,
                randomIntBetween(1, 10),
                1000,
                LuceneOperator.NO_LIMIT,
                false // no scoring
            );
            // TODO add index2
            MappedFieldType ft = mapperService(indexKey).fieldType("key");
            var readerFactory = new ValuesSourceReaderOperator.Factory(
                List.of(new ValuesSourceReaderOperator.FieldInfo("key", ElementType.INT, shardIdx -> {
                    seenShards.add(shardIdx);
                    return ft.blockLoader(blContext());
                })),
                readerShardContexts,
                0
            );
            DriverContext driverContext = driverContext();
            List<Page> results = drive(
                readerFactory.get(driverContext),
                CannedSourceOperator.collectPages(luceneFactory.get(driverContext)).iterator(),
                driverContext
            );
            assertThat(seenShards, equalTo(IntStream.range(0, shardCount).boxed().collect(Collectors.toCollection(TreeSet::new))));
            for (Page p : results) {
                IntBlock keyBlock = p.getBlock(1);
                IntVector keys = keyBlock.asVector();
                for (int i = 0; i < keys.getPositionCount(); i++) {
                    keyCounts.merge(keys.getInt(i), 1, Integer::sum);
                }
            }
            assertThat(keyCounts.keySet(), hasSize(size));
            for (int k = 0; k < size; k++) {
                assertThat(keyCounts.get(k), equalTo(shardCount));
            }
        } finally {
            IOUtils.close(closeMe);
        }
    }

    protected final List<Page> drive(Operator operator, Iterator<Page> input, DriverContext driverContext) {
        return drive(List.of(operator), input, driverContext);
    }

    protected final List<Page> drive(List<Operator> operators, Iterator<Page> input, DriverContext driverContext) {
        List<Page> results = new ArrayList<>();
        boolean success = false;
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(input),
                operators,
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            runDriver(d);
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(results.iterator(), p -> p::releaseBlocks)));
            }
        }
        return results;
    }

    public static void runDriver(Driver driver) {
        runDriver(List.of(driver));
    }

    public static void runDriver(List<Driver> drivers) {
        drivers = new ArrayList<>(drivers);
        int dummyDrivers = between(0, 10);
        for (int i = 0; i < dummyDrivers; i++) {
            drivers.add(
                TestDriverFactory.create(
                    new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance()),
                    new SequenceLongBlockSourceOperator(
                        TestBlockFactory.getNonBreakingInstance(),
                        LongStream.range(0, between(1, 100)),
                        between(1, 100)
                    ),
                    List.of(),
                    new PageConsumerOperator(Page::releaseBlocks)
                )
            );
        }
        Randomness.shuffle(drivers);
        int numThreads = between(1, 16);
        ThreadPool threadPool = new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(Settings.EMPTY, "esql", numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        var driverRunner = new DriverRunner(threadPool.getThreadContext()) {
            @Override
            protected void start(Driver driver, ActionListener<Void> driverListener) {
                Driver.start(threadPool.getThreadContext(), threadPool.executor("esql"), driver, between(1, 10000), driverListener);
            }
        };
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        try {
            driverRunner.runToCompletion(drivers, future);
            /*
             * We use a 3-minute timer because many of the cases can
             * take 40 seconds in CI. Locally it's taking 9 seconds.
             */
            future.actionGet(TimeValue.timeValueMinutes(3));
        } finally {
            terminate(threadPool);
        }
    }

    public static void assertDriverContext(DriverContext driverContext) {
        assertTrue(driverContext.isFinished());
        assertThat(driverContext.getSnapshot().releasables(), empty());
    }

    public static int randomPageSize() {
        if (randomBoolean()) {
            return between(1, 16);
        } else {
            return between(1, 16 * 1024);
        }
    }

    /**
     * This method will produce the same converter for all shards, which makes it useful for general type converting tests,
     * but not specifically union-types tests which require different converters for each shard.
     */
    private static BlockLoader getBlockLoaderFor(int shardIdx, MappedFieldType ft, MappedFieldType ftX) {
        if (shardIdx < 0 || shardIdx >= INDICES.size()) {
            fail("unexpected shardIdx [" + shardIdx + "]");
        }
        BlockLoader blockLoader = ft.blockLoader(blContext());
        if (ftX != null && ftX.typeName().equals(ft.typeName()) == false) {
            blockLoader = new TestTypeConvertingBlockLoader(blockLoader, ft.typeName(), ftX.typeName());
        } else {
            TestIndexMappingConfig mappingConfig = INDICES.get("index" + (shardIdx + 1));
            TestFieldType<?> testFieldType = mappingConfig.fieldTypes.get(ft.name());
            if (testFieldType != null) {
                blockLoader = new TestTypeConvertingBlockLoader(blockLoader, testFieldType.dataType.typeName(), "keyword");
            }
        }
        return blockLoader;
    }

    /**
     * This method is used to generate shard-specific field information, so we can have different types and BlockLoaders for each shard.
     */
    private BlockLoader getBlockLoaderFor(int shardIdx, String fieldName, DataType toType) {
        if (shardIdx < 0 || shardIdx >= INDICES.size()) {
            fail("unexpected shardIdx [" + shardIdx + "]");
        }
        String indexKey = "index" + (shardIdx + 1);
        TestIndexMappingConfig mappingConfig = INDICES.get(indexKey);
        TestFieldType<?> testFieldType = mappingConfig.fieldTypes.get(fieldName);
        if (testFieldType == null) {
            throw new IllegalArgumentException("Unknown test field: " + fieldName);
        }
        MapperService mapper = mapperService(indexKey);
        MappedFieldType ft = mapper.fieldType(fieldName);
        BlockLoader blockLoader = ft.blockLoader(blContext());
        blockLoader = new TestTypeConvertingBlockLoader(blockLoader, testFieldType.dataType.typeName(), toType.typeName());
        return blockLoader;
    }

    /**
     * The implementation of union-types relies on the <code>BlockLoader.convert(Block)</code> to convert the block to the correct type
     * at the point it is read from source, so that the rest of the query only deals with a single type for that field.
     * This is implemented in the 'esql' module, and so we have a mock for this behaviour here, which is a simplified subset of the
     * features in the real implementation.
     */
    static class TestTypeConvertingBlockLoader implements BlockLoader {
        protected final BlockLoader delegate;
        private final EvalOperator.ExpressionEvaluator convertEvaluator;

        protected TestTypeConvertingBlockLoader(BlockLoader delegate, String fromTypeName, String toTypeName) {
            this.delegate = delegate;
            DriverContext driverContext = new DriverContext(
                BigArrays.NON_RECYCLING_INSTANCE,
                new org.elasticsearch.compute.data.BlockFactory(
                    new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                    BigArrays.NON_RECYCLING_INSTANCE
                )
            );
            TestBlockConverter blockConverter = TestDataTypeConverters.blockConverter(driverContext, fromTypeName, toTypeName);
            this.convertEvaluator = new EvalOperator.ExpressionEvaluator() {
                @Override
                public org.elasticsearch.compute.data.Block eval(Page page) {
                    org.elasticsearch.compute.data.Block block = page.getBlock(0);
                    return blockConverter.convert(block);
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            // Return the delegates builder, which can build the original mapped type, before conversion
            return delegate.builder(factory, expectedCount);
        }

        @Override
        public Block convert(Block block) {
            Page page = new Page((org.elasticsearch.compute.data.Block) block);
            return convertEvaluator.eval(page);
        }

        @Override
        public ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) throws IOException {
            ColumnAtATimeReader reader = delegate.columnAtATimeReader(context);
            if (reader == null) {
                return null;
            }
            return new ColumnAtATimeReader() {
                @Override
                public Block read(BlockFactory factory, Docs docs) throws IOException {
                    Block block = reader.read(factory, docs);
                    Page page = new Page((org.elasticsearch.compute.data.Block) block);
                    return convertEvaluator.eval(page);
                }

                @Override
                public boolean canReuse(int startingDocID) {
                    return reader.canReuse(startingDocID);
                }

                @Override
                public String toString() {
                    return reader.toString();
                }
            };
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
            // We do no type conversion here, since that will be done in the ValueSourceReaderOperator for row-stride cases
            // Using the BlockLoader.convert(Block) function defined above
            return delegate.rowStrideReader(context);
        }

        @Override
        public StoredFieldsSpec rowStrideStoredFieldSpec() {
            return delegate.rowStrideStoredFieldSpec();
        }

        @Override
        public boolean supportsOrdinals() {
            // Fields with mismatching types cannot use ordinals for uniqueness determination, but must convert the values first
            return false;
        }

        @Override
        public SortedSetDocValues ordinals(LeafReaderContext context) {
            throw new IllegalArgumentException("Ordinals are not supported for type conversion");
        }

        @Override
        public final String toString() {
            return "TypeConvertingBlockLoader[delegate=" + delegate + "]";
        }
    }

    @FunctionalInterface
    private interface TestBlockConverter {
        Block convert(Block block);
    }

    /**
     * Blocks that should be converted from some type to a string (keyword) can use this converter.
     */
    private abstract static class BlockToStringConverter implements TestBlockConverter {
        private final DriverContext driverContext;

        BlockToStringConverter(DriverContext driverContext) {
            this.driverContext = driverContext;
        }

        @Override
        public Block convert(Block block) {
            int positionCount = block.getPositionCount();
            try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    int valueCount = block.getValueCount(p);
                    int start = block.getFirstValueIndex(p);
                    int end = start + valueCount;
                    boolean positionOpened = false;
                    boolean valuesAppended = false;
                    for (int i = start; i < end; i++) {
                        BytesRef value = evalValue(block, i);
                        if (positionOpened == false && valueCount > 1) {
                            builder.beginPositionEntry();
                            positionOpened = true;
                        }
                        builder.appendBytesRef(value);
                        valuesAppended = true;
                    }
                    if (valuesAppended == false) {
                        builder.appendNull();
                    } else if (positionOpened) {
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            } finally {
                block.close();
            }
        }

        abstract BytesRef evalValue(Block container, int index);
    }

    /**
     * Blocks that should be converted from a string (keyword) to some other type can use this converter.
     */
    private abstract static class TestBlockFromStringConverter<T> implements TestBlockConverter {
        protected final DriverContext driverContext;

        TestBlockFromStringConverter(DriverContext driverContext) {
            this.driverContext = driverContext;
        }

        @Override
        public Block convert(Block b) {
            BytesRefBlock block = (BytesRefBlock) b;
            int positionCount = block.getPositionCount();
            try (Block.Builder builder = blockBuilder(positionCount)) {
                BytesRef scratchPad = new BytesRef();
                for (int p = 0; p < positionCount; p++) {
                    int valueCount = block.getValueCount(p);
                    int start = block.getFirstValueIndex(p);
                    int end = start + valueCount;
                    boolean positionOpened = false;
                    boolean valuesAppended = false;
                    for (int i = start; i < end; i++) {
                        T value = evalValue(block, i, scratchPad);
                        if (positionOpened == false && valueCount > 1) {
                            builder.beginPositionEntry();
                            positionOpened = true;
                        }
                        appendValue(builder, value);
                        valuesAppended = true;
                    }
                    if (valuesAppended == false) {
                        builder.appendNull();
                    } else if (positionOpened) {
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            } finally {
                b.close();
            }
        }

        abstract Block.Builder blockBuilder(int expectedCount);

        abstract void appendValue(Block.Builder builder, T value);

        abstract T evalValue(BytesRefBlock container, int index, BytesRef scratchPad);
    }

    private static class TestLongBlockToStringConverter extends BlockToStringConverter {
        TestLongBlockToStringConverter(DriverContext driverContext) {
            super(driverContext);
        }

        @Override
        BytesRef evalValue(Block container, int index) {
            return new BytesRef(Long.toString(((LongBlock) container).getLong(index)));
        }
    }

    private static class TestLongBlockFromStringConverter extends TestBlockFromStringConverter<Long> {
        TestLongBlockFromStringConverter(DriverContext driverContext) {
            super(driverContext);
        }

        @Override
        Block.Builder blockBuilder(int expectedCount) {
            return driverContext.blockFactory().newLongBlockBuilder(expectedCount);
        }

        @Override
        Long evalValue(BytesRefBlock container, int index, BytesRef scratchPad) {
            return StringUtils.parseLong(container.getBytesRef(index, scratchPad).utf8ToString());
        }

        @Override
        void appendValue(Block.Builder builder, Long value) {
            ((LongBlock.Builder) builder).appendLong(value);
        }
    }

    private static class TestIntegerBlockToStringConverter extends BlockToStringConverter {
        TestIntegerBlockToStringConverter(DriverContext driverContext) {
            super(driverContext);
        }

        @Override
        BytesRef evalValue(Block container, int index) {
            return new BytesRef(Integer.toString(((IntBlock) container).getInt(index)));
        }
    }

    private static class TestIntegerBlockFromStringConverter extends TestBlockFromStringConverter<Integer> {
        TestIntegerBlockFromStringConverter(DriverContext driverContext) {
            super(driverContext);
        }

        @Override
        Block.Builder blockBuilder(int expectedCount) {
            return driverContext.blockFactory().newIntBlockBuilder(expectedCount);
        }

        @Override
        Integer evalValue(BytesRefBlock container, int index, BytesRef scratchPad) {
            return (int) StringUtils.parseLong(container.getBytesRef(index, scratchPad).utf8ToString());
        }

        @Override
        void appendValue(Block.Builder builder, Integer value) {
            ((IntBlock.Builder) builder).appendInt(value);
        }
    }

    private static class TestBooleanBlockToStringConverter extends BlockToStringConverter {

        TestBooleanBlockToStringConverter(DriverContext driverContext) {
            super(driverContext);
        }

        @Override
        BytesRef evalValue(Block container, int index) {
            return ((BooleanBlock) container).getBoolean(index) ? new BytesRef("true") : new BytesRef("false");
        }
    }

    private static class TestBooleanBlockFromStringConverter extends TestBlockFromStringConverter<Boolean> {

        TestBooleanBlockFromStringConverter(DriverContext driverContext) {
            super(driverContext);
        }

        @Override
        Block.Builder blockBuilder(int expectedCount) {
            return driverContext.blockFactory().newBooleanBlockBuilder(expectedCount);
        }

        @Override
        void appendValue(Block.Builder builder, Boolean value) {
            ((BooleanBlock.Builder) builder).appendBoolean(value);
        }

        @Override
        Boolean evalValue(BytesRefBlock container, int index, BytesRef scratchPad) {
            return Boolean.parseBoolean(container.getBytesRef(index, scratchPad).utf8ToString());
        }
    }

    private static class TestDoubleBlockToStringConverter extends BlockToStringConverter {

        TestDoubleBlockToStringConverter(DriverContext driverContext) {
            super(driverContext);
        }

        @Override
        BytesRef evalValue(Block container, int index) {
            return new BytesRef(Double.toString(((DoubleBlock) container).getDouble(index)));
        }
    }

    private static class TestDoubleBlockFromStringConverter extends TestBlockFromStringConverter<Double> {

        TestDoubleBlockFromStringConverter(DriverContext driverContext) {
            super(driverContext);
        }

        @Override
        Block.Builder blockBuilder(int expectedCount) {
            return driverContext.blockFactory().newDoubleBlockBuilder(expectedCount);
        }

        @Override
        void appendValue(Block.Builder builder, Double value) {
            ((DoubleBlock.Builder) builder).appendDouble(value);
        }

        @Override
        Double evalValue(BytesRefBlock container, int index, BytesRef scratchPad) {
            return Double.parseDouble(container.getBytesRef(index, scratchPad).utf8ToString());
        }
    }

    /**
     * Many types are backed by BytesRef block, but encode their contents in different ways.
     * For example, the IP type has a 16-byte block that encodes both IPv4 and IPv6 as 16byte-IPv6 binary byte arrays.
     * But the KEYWORD type has a BytesRef block that encodes the keyword as a UTF-8 string,
     * and it typically has a much shorter length for IP data, for example, "192.168.0.1" is 11 bytes.
     * Converting blocks between these types involves converting the BytesRef block to the specific internal type,
     * and then back to a BytesRef block with the other encoding.
     */
    private abstract static class TestBytesRefToBytesRefConverter extends BlockToStringConverter {

        BytesRef scratchPad = new BytesRef();

        TestBytesRefToBytesRefConverter(DriverContext driverContext) {
            super(driverContext);
        }

        @Override
        BytesRef evalValue(Block container, int index) {
            return convertByteRef(((BytesRefBlock) container).getBytesRef(index, scratchPad));
        }

        abstract BytesRef convertByteRef(BytesRef bytesRef);
    }

    private static class TestIPToStringConverter extends TestBytesRefToBytesRefConverter {

        TestIPToStringConverter(DriverContext driverContext) {
            super(driverContext);
        }

        @Override
        BytesRef convertByteRef(BytesRef bytesRef) {
            return new BytesRef(DocValueFormat.IP.format(bytesRef));
        }
    }

    private static class TestStringToIPConverter extends TestBytesRefToBytesRefConverter {

        TestStringToIPConverter(DriverContext driverContext) {
            super(driverContext);
        }

        @Override
        BytesRef convertByteRef(BytesRef bytesRef) {
            return StringUtils.parseIP(bytesRef.utf8ToString());
        }
    }

    /**
     * Utility class for creating type-specific converters based on their typeNamne values.
     * We do not support all possibly combinations, but only those that are needed for the tests.
     * In particular, either the 'from' or 'to' types must be KEYWORD.
     */
    private static class TestDataTypeConverters {
        public static TestBlockConverter blockConverter(DriverContext driverContext, String fromTypeName, String toTypeName) {
            if (toTypeName == null || fromTypeName.equals(toTypeName)) {
                return b -> b;
            }
            if (isString(fromTypeName)) {
                return switch (toTypeName) {
                    case "boolean" -> new TestBooleanBlockFromStringConverter(driverContext);
                    case "short", "integer" -> new TestIntegerBlockFromStringConverter(driverContext);
                    case "long" -> new TestLongBlockFromStringConverter(driverContext);
                    case "double", "float" -> new TestDoubleBlockFromStringConverter(driverContext);
                    case "ip" -> new TestStringToIPConverter(driverContext);
                    default -> throw new UnsupportedOperationException("Conversion from string to " + toTypeName + " is not supported");
                };
            }
            if (isString(toTypeName)) {
                return switch (fromTypeName) {
                    case "boolean" -> new TestBooleanBlockToStringConverter(driverContext);
                    case "short", "integer" -> new TestIntegerBlockToStringConverter(driverContext);
                    case "long" -> new TestLongBlockToStringConverter(driverContext);
                    case "double", "float" -> new TestDoubleBlockToStringConverter(driverContext);
                    case "ip" -> new TestIPToStringConverter(driverContext);
                    default -> throw new UnsupportedOperationException("Conversion from " + fromTypeName + " to string is not supported");
                };
            }
            throw new UnsupportedOperationException("Conversion from " + fromTypeName + " to " + toTypeName + " is not supported");
        }

        private static boolean isString(String typeName) {
            return typeName.equals("keyword") || typeName.equals("text");
        }
    }
}
