/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

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
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.LuceneOperator;
import org.elasticsearch.compute.lucene.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.LuceneSourceOperatorTests;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorTests.Checks;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorTests.FieldCase;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorTests.StatusChecks;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.AnyOperatorTestCase;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.blockloader.ConstantBytes;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
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

import static org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorTests.blContext;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
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
 * Since this test does not have access to the type conversion code in the ESQL module, we have mocks for that behaviour.
 * in the inner classes.
 */
@SuppressWarnings("resource")
public class ValueSourceReaderTypeConversionTests extends AnyOperatorTestCase {
    private static final String[] PREFIX = new String[] { "a", "b", "c" };
    private static final Map<String, TestIndexMappingConfig> INDICES = new LinkedHashMap<>();
    static {
        addIndex(
            Map.of(
                "ip",
                new TestFieldType<>("ip", "ip", d -> "192.169.0." + d % 256, UnionChecks::unionIPsAsStrings),
                "duration",
                new TestFieldType<>("duration", "long", d -> (long) d, UnionChecks::unionDurationsAsStrings)
            )
        );
        addIndex(
            Map.of(
                "ip",
                new TestFieldType<>("ip", "keyword", d -> "192.169.0." + d % 256, UnionChecks::unionIPsAsStrings),
                "duration",
                new TestFieldType<>("duration", "keyword", d -> Integer.toString(d), UnionChecks::unionDurationsAsStrings)
            )
        );
    }

    static void addIndex(Map<String, TestFieldType<?>> fieldTypes) {
        String indexKey = "index" + (INDICES.size() + 1);
        INDICES.put(indexKey, new TestIndexMappingConfig(indexKey, INDICES.size(), fieldTypes));
    }

    private record TestIndexMappingConfig(String indexName, int shardIdx, Map<String, TestFieldType<?>> fieldTypes) {}

    private record TestFieldType<T>(
        String name,
        String typeName,
        Function<Integer, T> valueGenerator,
        ValuesSourceReaderOperatorTests.CheckResultsWithIndexKey checkResults
    ) {}

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
            .map(index -> new ValuesSourceReaderOperator.ShardContext(reader(index), (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE, 0.2))
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
        return factory(shardContexts, ft.name(), elementType, ValuesSourceReaderOperator.load(ft.blockLoader(blContext())));
    }

    private static Operator.OperatorFactory factory(
        List<ValuesSourceReaderOperator.ShardContext> shardContexts,
        String name,
        ElementType elementType,
        ValuesSourceReaderOperator.LoaderAndConverter loaderAndConverter
    ) {
        return new ValuesSourceReaderOperator.Factory(
            ByteSizeValue.ofGb(1),
            List.of(new ValuesSourceReaderOperator.FieldInfo(name, elementType, false, shardIdx -> {
                if (shardIdx < 0 || shardIdx >= INDICES.size()) {
                    fail("unexpected shardIdx [" + shardIdx + "]");
                }
                return loaderAndConverter;
            })),
            new IndexedByShardIdFromList<>(shardContexts),
            randomBoolean(),
            0
        );
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
            new IndexedByShardIdFromList<>(shardContexts),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())),
            DataPartitioning.SHARD,
            DataPartitioning.AutoStrategy.DEFAULT,
            1,// randomIntBetween(1, 10),
            pageSize,
            LuceneOperator.NO_LIMIT,
            false // no scoring
        );
        return luceneFactory.get(context);
    }

    private void initMapping(String indexKey) throws IOException {
        TestIndexMappingConfig indexMappingConfig = INDICES.get(indexKey);
        mapperServices.put(indexKey, new MapperServiceTestCase() {}.createMapperService(MapperServiceTestCase.mapping(b -> {
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
                simpleField(b, fieldName, fieldType.typeName);
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
        FieldCase testCase = fc(new KeywordFieldMapper.KeywordFieldType("kwd"), ElementType.BYTES_REF).results(UnionChecks::tags)
            .readers(StatusChecks::keywordsFromDocValues);
        // TODO: Add index2
        operators.add(
            new ValuesSourceReaderOperator.Factory(
                ByteSizeValue.ofGb(1),
                List.of(testCase.info(), fieldInfo(mapperService(indexKey).fieldType("key"), ElementType.INT)),
                new IndexedByShardIdFromList<>(shardContexts),
                randomBoolean(),
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
                testCase.results().check(page.getBlock(1), p, key, indexKey);
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
            shuffledBlocks[b] = source.getBlock(b).filter(false, shuffleArray);
        }
        source = new Page(shuffledBlocks);
        loadSimpleAndAssert(driverContext, List.of(source), Block.MvOrdering.UNORDERED, Block.MvOrdering.UNORDERED);
    }

    private static ValuesSourceReaderOperator.FieldInfo fieldInfo(MappedFieldType ft, ElementType elementType) {
        return new ValuesSourceReaderOperator.FieldInfo(ft.name(), elementType, false, shardIdx -> getBlockLoaderFor(shardIdx, ft, null));
    }

    private ValuesSourceReaderOperator.FieldInfo fieldInfo(String fieldName, ElementType elementType, String toType) {
        return new ValuesSourceReaderOperator.FieldInfo(
            fieldName,
            elementType,
            false,
            shardIdx -> getBlockLoaderFor(shardIdx, fieldName, toType)
        );
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
                ByteSizeValue.ofGb(1),
                List.of(
                    fieldInfo(mapperService("index1").fieldType("key"), ElementType.INT),
                    fieldInfo(mapperService("index1").fieldType("indexKey"), ElementType.BYTES_REF)
                ),
                new IndexedByShardIdFromList<>(shardContexts),
                randomBoolean(),
                0
            ).get(driverContext)
        );
        List<FieldCase> tests = new ArrayList<>();
        while (cases.isEmpty() == false) {
            List<FieldCase> b = randomNonEmptySubsetOf(cases);
            cases.removeAll(b);
            tests.addAll(b);
            operators.add(
                new ValuesSourceReaderOperator.Factory(
                    ByteSizeValue.ofGb(1),
                    b.stream().map(i -> i.info()).toList(),
                    new IndexedByShardIdFromList<>(shardContexts),
                    randomBoolean(),
                    0
                ).get(driverContext)
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
                        tests.get(i).results().check(page.getBlock(3 + i), p, key, indexKey);
                    } catch (AssertionError e) {
                        throw new AssertionError("error checking " + tests.get(i).info().name() + "[" + p + "]: " + e.getMessage(), e);
                    }
                }
            }
        }
        for (Operator op : operators) {
            ValuesSourceReaderOperatorStatus status = (ValuesSourceReaderOperatorStatus) op.status();
            assertThat(status.pagesReceived(), equalTo(input.size()));
            assertThat(status.pagesEmitted(), equalTo(input.size()));
        }
        assertDriverContext(driverContext);
    }

    private FieldCase fc(MapperService mapperService, String name, ElementType elementType) {
        return fc(mapperService.fieldType(name), elementType);
    }

    private static FieldCase fc(MappedFieldType ft, ElementType elementType) {
        return new FieldCase(fieldInfo(ft, elementType));
    }

    private static FieldCase fc(MapperService mapperService, String f1, String f2, ElementType elementType) {
        MappedFieldType ft1 = mapperService.fieldType(f1);
        MappedFieldType ft2 = mapperService.fieldType(f2);
        return new FieldCase(
            new ValuesSourceReaderOperator.FieldInfo(f1, elementType, false, shardIdx -> getBlockLoaderFor(shardIdx, ft1, ft2))
        );
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
            .map(
                i -> new ValuesSourceReaderOperator.Factory(
                    ByteSizeValue.ofGb(1),
                    List.of(i.info()),
                    new IndexedByShardIdFromList<>(shardContexts),
                    randomBoolean(),
                    0
                ).get(driverContext)
            )
            .toList();
        if (allInOnePage) {
            input = List.of(CannedSourceOperator.mergePages(input));
        }
        drive(operators, input.iterator(), driverContext);
        for (int i = 0; i < cases.size(); i++) {
            ValuesSourceReaderOperatorStatus status = (ValuesSourceReaderOperatorStatus) operators.get(i).status();
            assertThat(status.pagesReceived(), equalTo(input.size()));
            assertThat(status.pagesEmitted(), equalTo(input.size()));
            FieldCase fc = cases.get(i);
            fc.readers().check(fc.info().name(), allInOnePage, input.size(), totalSize, status.readersBuilt());
        }
    }

    private List<FieldCase> infoAndChecksForEachType(
        Block.MvOrdering booleanAndNumericalDocValuesMvOrdering,
        Block.MvOrdering bytesRefDocValuesMvOrdering
    ) {
        MapperService mapperService = mapperService("index1");  // almost fields have identical mapper service
        Checks checks = new Checks(booleanAndNumericalDocValuesMvOrdering, bytesRefDocValuesMvOrdering);
        List<FieldCase> r = new ArrayList<>();
        r.add(fc(mapperService, IdFieldMapper.NAME, ElementType.BYTES_REF).results(checks::ids).readers(StatusChecks::id));
        r.add(fc(TsidExtractingIdFieldMapper.INSTANCE.fieldType(), ElementType.BYTES_REF).results(checks::ids).readers(StatusChecks::id));
        r.add(fc(mapperService, "long", ElementType.LONG).results(checks::longs).readers(StatusChecks::longsFromDocValues));
        r.add(fc(mapperService, "str_long", "long", ElementType.LONG).results(checks::longs).readers(StatusChecks::strFromDocValues));
        r.add(
            fc(mapperService, "mv_long", ElementType.LONG).results(checks::mvLongsFromDocValues).readers(StatusChecks::mvLongsFromDocValues)
        );
        r.add(fc(mapperService, "missing_long", ElementType.LONG).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(fc(mapperService, "source_long", ElementType.LONG).results(checks::longs).readers(StatusChecks::longsFromSource));
        r.add(
            fc(mapperService, "mv_source_long", ElementType.LONG).results(checks::mvLongsUnordered).readers(StatusChecks::mvLongsFromSource)
        );
        r.add(fc(mapperService, "int", ElementType.INT).results(checks::ints).readers(StatusChecks::intsFromDocValues));
        r.add(fc(mapperService, "str_int", "int", ElementType.INT).results(checks::ints).readers(StatusChecks::strFromDocValues));
        r.add(fc(mapperService, "mv_int", ElementType.INT).results(checks::mvIntsFromDocValues).readers(StatusChecks::mvIntsFromDocValues));
        r.add(fc(mapperService, "missing_int", ElementType.INT).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(fc(mapperService, "source_int", ElementType.INT).results(checks::ints).readers(StatusChecks::intsFromSource));
        r.add(fc(mapperService, "mv_source_int", ElementType.INT).results(checks::mvIntsUnordered).readers(StatusChecks::mvIntsFromSource));
        r.add(fc(mapperService, "short", ElementType.INT).results(checks::shorts).readers(StatusChecks::shortsFromDocValues));
        r.add(fc(mapperService, "str_short", "short", ElementType.INT).results(checks::shorts).readers(StatusChecks::strFromDocValues));
        r.add(fc(mapperService, "mv_short", ElementType.INT).results(checks::mvShorts).readers(StatusChecks::mvShortsFromDocValues));
        r.add(fc(mapperService, "missing_short", ElementType.INT).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(fc(mapperService, "byte", ElementType.INT).results(checks::bytes).readers(StatusChecks::bytesFromDocValues));
        // r.add(fc(mapperService, "str_byte", ElementType.INT).results(checks::bytes).readers(StatusChecks::bytesFromDocValues));
        r.add(fc(mapperService, "mv_byte", ElementType.INT).results(checks::mvBytes).readers(StatusChecks::mvBytesFromDocValues));
        r.add(fc(mapperService, "missing_byte", ElementType.INT).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(fc(mapperService, "double", ElementType.DOUBLE).results(checks::doubles).readers(StatusChecks::doublesFromDocValues));
        r.add(
            fc(mapperService, "str_double", "double", ElementType.DOUBLE).results(checks::doubles).readers(StatusChecks::strFromDocValues)
        );
        r.add(fc(mapperService, "mv_double", ElementType.DOUBLE).results(checks::mvDoubles).readers(StatusChecks::mvDoublesFromDocValues));
        r.add(fc(mapperService, "missing_double", ElementType.DOUBLE).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(fc(mapperService, "kwd", ElementType.BYTES_REF).results(UnionChecks::tags).readers(StatusChecks::keywordsFromDocValues));
        r.add(
            fc(mapperService, "mv_kwd", ElementType.BYTES_REF).results(checks::mvStringsFromDocValues)
                .readers(StatusChecks::mvKeywordsFromDocValues)
        );
        r.add(fc(mapperService, "missing_kwd", ElementType.BYTES_REF).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(
            fc(storedKeywordField("stored_kwd"), ElementType.BYTES_REF).results(checks::strings).readers(StatusChecks::keywordsFromStored)
        );
        r.add(
            fc(storedKeywordField("mv_stored_kwd"), ElementType.BYTES_REF).results(checks::mvStringsUnordered)
                .readers(StatusChecks::mvKeywordsFromStored)
        );
        r.add(fc(mapperService, "source_kwd", ElementType.BYTES_REF).results(checks::strings).readers(StatusChecks::keywordsFromSource));
        r.add(
            fc(mapperService, "mv_source_kwd", ElementType.BYTES_REF).results(checks::mvStringsUnordered)
                .readers(StatusChecks::mvKeywordsFromSource)
        );
        r.add(
            new FieldCase(
                new ValuesSourceReaderOperator.FieldInfo(
                    "constant_bytes",
                    ElementType.BYTES_REF,
                    false,
                    shardIdx -> ValuesSourceReaderOperator.load(new ConstantBytes(new BytesRef("foo")))
                )
            ).results(checks::constantBytes).readers(StatusChecks::constantBytes)
        );
        r.add(
            new FieldCase(
                new ValuesSourceReaderOperator.FieldInfo(
                    "null",
                    ElementType.NULL,
                    false,
                    shardIdx -> ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS
                )
            ).results(checks::constantNulls).readers(StatusChecks::constantNulls)
        );

        // We only care about the field name at this point, so we can use any index mapper here
        TestIndexMappingConfig indexMappingConfig = INDICES.get("index1");
        for (TestFieldType<?> fieldType : indexMappingConfig.fieldTypes.values()) {
            r.add(
                new FieldCase(fieldInfo(fieldType.name, ElementType.BYTES_REF, "keyword")).results(fieldType.checkResults)
                    .readers(StatusChecks::unionFromDocValues)
            );
        }
        Collections.shuffle(r, random());
        return r;
    }

    static class UnionChecks {
        static void tags(Block block, int position, int key, String indexKey) {
            BytesRefVector keywords = ((BytesRefBlock) block).asVector();
            Object[] validTags = INDICES.keySet().stream().map(keyToTags::get).map(t -> t.get(key)).toArray();
            assertThat(keywords.getBytesRef(position, new BytesRef()).utf8ToString(), oneOf(validTags));
        }

        static void unionIPsAsStrings(Block block, int position, int key, String indexKey) {
            BytesRefVector keywords = ((BytesRefBlock) block).asVector();
            BytesRef bytesRef = keywords.getBytesRef(position, new BytesRef());
            TestIndexMappingConfig mappingConfig = INDICES.get(indexKey);
            TestFieldType<?> fieldType = mappingConfig.fieldTypes.get("ip");
            String expected = fieldType.valueGenerator.apply(key).toString();
            // Conversion should already be done in FieldInfo!
            // BytesRef found = (fieldType.typeName.equals("ip")) ? new BytesRef(DocValueFormat.IP.format(bytesRef)) : bytesRef;
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
    }

    public void testWithNulls() throws IOException {
        String indexKey = "index1";
        mapperServices.put(indexKey, new MapperServiceTestCase() {}.createMapperService(MapperServiceTestCase.mapping(b -> {
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
            new IndexedByShardIdFromSingleton<>(shardContext),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())),
            randomFrom(DataPartitioning.values()),
            DataPartitioning.AutoStrategy.DEFAULT,
            randomIntBetween(1, 10),
            randomPageSize(),
            LuceneOperator.NO_LIMIT,
            false // no scoring
        );
        var vsShardContext = new ValuesSourceReaderOperator.ShardContext(
            reader(indexKey),
            (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE,
            0.2
        );
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
            new TestDriverRunner().run(driver);
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
        return new KeywordFieldMapper.KeywordFieldType(name, ft, false);
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
                        ByteSizeValue.ofGb(1),
                        List.of(
                            new ValuesSourceReaderOperator.FieldInfo(
                                "null1",
                                ElementType.NULL,
                                false,
                                shardIdx -> ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS
                            ),
                            new ValuesSourceReaderOperator.FieldInfo(
                                "null2",
                                ElementType.NULL,
                                false,
                                shardIdx -> ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS
                            )
                        ),
                        new IndexedByShardIdFromList<>(shardContexts),
                        randomBoolean(),
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
            new TestDriverRunner().run(d);
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
            ByteSizeValue.ofGb(1),
            cases.stream().map(c -> c.info()).toList(),
            new IndexedByShardIdFromSingleton<>(
                new ValuesSourceReaderOperator.ShardContext(reader(indexKey), (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE, 0.2)
            ),
            randomBoolean(),
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
                    new ValuesSourceReaderOperator.ShardContext(readers[s], (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE, 0.2)
                );
            }
            var luceneFactory = new LuceneSourceOperator.Factory(
                new IndexedByShardIdFromList<>(contexts),
                ctx -> List.of(new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())),
                DataPartitioning.SHARD,
                DataPartitioning.AutoStrategy.DEFAULT,
                randomIntBetween(1, 10),
                1000,
                LuceneOperator.NO_LIMIT,
                false // no scoring
            );
            // TODO add index2
            MappedFieldType ft = mapperService(indexKey).fieldType("key");
            var readerFactory = new ValuesSourceReaderOperator.Factory(
                ByteSizeValue.ofGb(1),
                List.of(new ValuesSourceReaderOperator.FieldInfo("key", ElementType.INT, false, shardIdx -> {
                    seenShards.add(shardIdx);
                    return ValuesSourceReaderOperator.load(ft.blockLoader(blContext()));
                })),
                new IndexedByShardIdFromList<>(readerShardContexts),
                randomBoolean(),
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
            new TestDriverRunner().run(d);
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(results.iterator(), p -> p::releaseBlocks)));
            }
        }
        return results;
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
    private static ValuesSourceReaderOperator.LoaderAndConverter getBlockLoaderFor(int shardIdx, MappedFieldType ft, MappedFieldType ftX) {
        if (shardIdx < 0 || shardIdx >= INDICES.size()) {
            fail("unexpected shardIdx [" + shardIdx + "]");
        }
        BlockLoader blockLoader = ft.blockLoader(blContext());
        if (ftX != null && ftX.typeName().equals(ft.typeName()) == false) {
            return ValuesSourceReaderOperator.loadAndConvert(
                blockLoader,
                TestDataTypeConverters.converterFactory(ft.typeName(), ftX.typeName())
            );
        } else {
            TestIndexMappingConfig mappingConfig = INDICES.get("index" + (shardIdx + 1));
            TestFieldType<?> testFieldType = mappingConfig.fieldTypes.get(ft.name());
            if (testFieldType != null) {
                return ValuesSourceReaderOperator.loadAndConvert(
                    blockLoader,
                    TestDataTypeConverters.converterFactory(testFieldType.typeName, "keyword")
                );
            }
        }
        return ValuesSourceReaderOperator.load(blockLoader);
    }

    /**
     * This method is used to generate shard-specific field information, so we can have different types and BlockLoaders for each shard.
     */
    private ValuesSourceReaderOperator.LoaderAndConverter getBlockLoaderFor(int shardIdx, String fieldName, String toType) {
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
        return ValuesSourceReaderOperator.loadAndConvert(
            blockLoader,
            TestDataTypeConverters.converterFactory(testFieldType.typeName, toType)
        );
    }

    @FunctionalInterface
    private interface TestBlockConverter {
        Block convert(Block block);
    }

    /**
     * Blocks that should be converted from some type to a string (keyword) can use this converter.
     */
    private abstract static class BlockToStringConverter implements ValuesSourceReaderOperator.ConverterEvaluator {
        private final DriverContext driverContext;

        BlockToStringConverter(DriverContext driverContext) {
            this.driverContext = driverContext;
            driverContext.breaker().addEstimateBytesAndMaybeBreak(1, "test");
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

        @Override
        public void close() {
            driverContext.breaker().addWithoutBreaking(-1);
        }

        abstract BytesRef evalValue(Block container, int index);
    }

    /**
     * Blocks that should be converted from a string (keyword) to some other type can use this converter.
     */
    private abstract static class TestBlockFromStringConverter<T> implements ValuesSourceReaderOperator.ConverterEvaluator {
        protected final DriverContext driverContext;

        TestBlockFromStringConverter(DriverContext driverContext) {
            this.driverContext = driverContext;
            driverContext.breaker().addEstimateBytesAndMaybeBreak(1, "test");
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

        @Override
        public void close() {
            driverContext.breaker().addWithoutBreaking(-1);
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
            return Booleans.parseBoolean(container.getBytesRef(index, scratchPad).utf8ToString());
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
     * Utility class for creating type-specific converters based on their typeName values.
     * We do not support all possibly combinations, but only those that are needed for the tests.
     * In particular, either the 'from' or 'to' types must be KEYWORD.
     */
    private static class TestDataTypeConverters {
        public static ValuesSourceReaderOperator.ConverterFactory converterFactory(String fromTypeName, String toTypeName) {
            return context -> TestDataTypeConverters.converter(context, fromTypeName, toTypeName);
        }

        private static ValuesSourceReaderOperator.ConverterEvaluator converter(
            DriverContext driverContext,
            String fromTypeName,
            String toTypeName
        ) {
            if (toTypeName == null || fromTypeName.equals(toTypeName)) {
                return null;
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
