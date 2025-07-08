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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.mockfile.HandleLimitFS;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.lucene.LuceneOperator;
import org.elasticsearch.compute.lucene.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.LuceneSourceOperatorTests;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.core.IOUtils;
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
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests for {@link ValuesSourceReaderOperator}. Turns off {@link HandleLimitFS}
 * because it can make a large number of documents and doesn't want to allow
 * lucene to merge. It intentionally tries to create many files to make sure
 * that {@link ValuesSourceReaderOperator} works with it.
 */
@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS")
public class ValuesSourceReaderOperatorTests extends OperatorTestCase {
    private static final String[] PREFIX = new String[] { "a", "b", "c" };
    private static final boolean[][] BOOLEANS = new boolean[][] {
        { true },
        { false, true },
        { false, true, true },
        { false, false, true, true } };

    static final double STORED_FIELDS_SEQUENTIAL_PROPORTIONS = 0.2;

    private Directory directory = newDirectory();
    private MapperService mapperService;
    private IndexReader reader;
    private static final Map<Integer, String> keyToTags = new HashMap<>();

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        if (reader == null) {
            // Init a reader if one hasn't been built, so things don't blow up
            try {
                initIndex(100, 10);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return factory(reader, mapperService.fieldType("long"), ElementType.LONG);
    }

    public static Operator.OperatorFactory factory(IndexReader reader, MappedFieldType ft, ElementType elementType) {
        return factory(reader, ft.name(), elementType, ft.blockLoader(blContext()));
    }

    static Operator.OperatorFactory factory(IndexReader reader, String name, ElementType elementType, BlockLoader loader) {
        return new ValuesSourceReaderOperator.Factory(List.of(new ValuesSourceReaderOperator.FieldInfo(name, elementType, shardIdx -> {
            if (shardIdx != 0) {
                fail("unexpected shardIdx [" + shardIdx + "]");
            }
            return loader;
        })),
            List.of(
                new ValuesSourceReaderOperator.ShardContext(
                    reader,
                    () -> SourceLoader.FROM_STORED_SOURCE,
                    STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                )
            ),
            0
        );
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return simpleInput(driverContext(), size, commitEvery(size), randomPageSize());
    }

    private int commitEvery(int numDocs) {
        return Math.max(1, (int) Math.ceil((double) numDocs / 10));
    }

    private SourceOperator simpleInput(DriverContext context, int size, int commitEvery, int pageSize) {
        try {
            initIndex(size, commitEvery);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sourceOperator(context, pageSize);
    }

    private SourceOperator sourceOperator(DriverContext context, int pageSize) {
        var luceneFactory = new LuceneSourceOperator.Factory(
            List.of(new LuceneSourceOperatorTests.MockShardContext(reader, 0)),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(new MatchAllDocsQuery(), List.of())),
            DataPartitioning.SHARD,
            randomIntBetween(1, 10),
            pageSize,
            LuceneOperator.NO_LIMIT,
            false // no scoring
        );
        return luceneFactory.get(context);
    }

    private void initMapping() throws IOException {
        mapperService = new MapperServiceTestCase() {
        }.createMapperService(MapperServiceTestCase.mapping(b -> {
            fieldExamples(b, "key", "integer");
            fieldExamples(b, "int", "integer");
            fieldExamples(b, "short", "short");
            fieldExamples(b, "byte", "byte");
            fieldExamples(b, "long", "long");
            fieldExamples(b, "double", "double");
            fieldExamples(b, "bool", "boolean");
            fieldExamples(b, "kwd", "keyword");
            b.startObject("stored_kwd").field("type", "keyword").field("store", true).endObject();
            b.startObject("mv_stored_kwd").field("type", "keyword").field("store", true).endObject();

            simpleField(b, "missing_text", "text");
            b.startObject("source_text").field("type", "text").field("store", false).endObject();
            b.startObject("mv_source_text").field("type", "text").field("store", false).endObject();
            b.startObject("long_source_text").field("type", "text").field("store", false).endObject();
            b.startObject("stored_text").field("type", "text").field("store", true).endObject();
            b.startObject("mv_stored_text").field("type", "text").field("store", true).endObject();

            for (String n : new String[] { "text_with_delegate", "mv_text_with_delegate", "missing_text_with_delegate" }) {
                b.startObject(n);
                {
                    b.field("type", "text");
                    b.startObject("fields");
                    b.startObject("kwd").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
            }
        }));
    }

    private void initIndex(int size, int commitEvery) throws IOException {
        initMapping();
        keyToTags.clear();
        reader = initIndex(directory, size, commitEvery);
    }

    private IndexReader initIndex(Directory directory, int size, int commitEvery) throws IOException {
        try (
            IndexWriter writer = new IndexWriter(
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            )
        ) {
            for (int d = 0; d < size; d++) {
                XContentBuilder source = JsonXContent.contentBuilder();
                source.startObject();
                source.field("key", d);

                source.field("long", d);
                source.startArray("mv_long");
                for (int v = 0; v <= d % 3; v++) {
                    source.value((long) (-1_000 * d + v));
                }
                source.endArray();
                source.field("source_long", (long) d);
                source.startArray("mv_source_long");
                for (int v = 0; v <= d % 3; v++) {
                    source.value((long) (-1_000 * d + v));
                }
                source.endArray();

                source.field("int", d);
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

                source.field("bool", d % 2 == 0);
                source.startArray("mv_bool");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(v % 2 == 0);
                }
                source.endArray();
                source.field("source_bool", d % 2 == 0);
                source.startArray("source_mv_bool");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(v % 2 == 0);
                }
                source.endArray();

                String tag = keyToTags.computeIfAbsent(d, k -> "tag-" + randomIntBetween(1, 5));
                source.field("kwd", tag);
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
                source.field("stored_text", Integer.toString(d));
                source.startArray("mv_stored_text");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(PREFIX[v] + d);
                }
                source.endArray();
                source.field("source_text", Integer.toString(d));
                source.startArray("mv_source_text");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(PREFIX[v] + d);
                }
                source.endArray();

                source.field("text_with_delegate", Integer.toString(d));
                source.startArray("mv_text_with_delegate");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(PREFIX[v] + d);
                }
                source.endArray();

                source.endObject();

                ParsedDocument doc = mapperService.documentParser()
                    .parseDocument(
                        new SourceToParse("id" + d, BytesReference.bytes(source), XContentType.JSON),
                        mapperService.mappingLookup()
                    );
                writer.addDocuments(doc.docs());

                if (d % commitEvery == commitEvery - 1) {
                    writer.commit();
                }
            }
        }
        return DirectoryReader.open(directory);
    }

    private IndexReader initIndexLongField(Directory directory, int size, int commitEvery) throws IOException {
        try (
            IndexWriter writer = new IndexWriter(
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            )
        ) {
            for (int d = 0; d < size; d++) {
                XContentBuilder source = JsonXContent.contentBuilder();
                source.startObject();
                source.field("long_source_text", Integer.toString(d).repeat(100 * 1024));
                source.endObject();
                ParsedDocument doc = mapperService.documentParser()
                    .parseDocument(
                        new SourceToParse("id" + d, BytesReference.bytes(source), XContentType.JSON),
                        mapperService.mappingLookup()
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

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        long expectedSum = 0;
        long current = 0;

        long sum = 0;
        for (Page r : results) {
            LongBlock b = r.getBlock(r.getBlockCount() - 1);
            for (int p = 0; p < b.getPositionCount(); p++) {
                expectedSum += current;
                current++;
                sum += b.getLong(p);
            }
        }

        assertThat(sum, equalTo(expectedSum));
    }

    @Override
    protected ByteSizeValue enoughMemoryForSimple() {
        assumeFalse("strange exception in the test, fix soon", true);
        return ByteSizeValue.ofKb(1);
    }

    public void testLoadAll() {
        DriverContext driverContext = driverContext();
        loadSimpleAndAssert(
            driverContext,
            CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), between(100, 5000))),
            Block.MvOrdering.SORTED_ASCENDING,
            Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
        );
    }

    public void testLoadAllInOnePage() {
        DriverContext driverContext = driverContext();
        loadSimpleAndAssert(
            driverContext,
            List.of(
                CannedSourceOperator.mergePages(
                    CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), between(100, 5000)))
                )
            ),
            Block.MvOrdering.UNORDERED,
            Block.MvOrdering.UNORDERED
        );
    }

    public void testManySingleDocPages() {
        DriverContext driverContext = driverContext();
        int numDocs = between(10, 100);
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext, numDocs, between(1, numDocs), 1));
        Randomness.shuffle(input);
        List<Operator> operators = new ArrayList<>();
        Checks checks = new Checks(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING, Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
        FieldCase testCase = new FieldCase(
            new KeywordFieldMapper.KeywordFieldType("kwd"),
            ElementType.BYTES_REF,
            checks::tags,
            StatusChecks::keywordsFromDocValues
        );
        operators.add(
            new ValuesSourceReaderOperator.Factory(
                List.of(testCase.info, fieldInfo(mapperService.fieldType("key"), ElementType.INT)),
                List.of(
                    new ValuesSourceReaderOperator.ShardContext(
                        reader,
                        () -> SourceLoader.FROM_STORED_SOURCE,
                        STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                    )
                ),
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
                testCase.checkResults.check(page.getBlock(1), p, key);
            }
        }
    }

    public void testEmpty() {
        DriverContext driverContext = driverContext();
        loadSimpleAndAssert(
            driverContext,
            CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), 0)),
            Block.MvOrdering.UNORDERED,
            Block.MvOrdering.UNORDERED
        );
    }

    public void testLoadAllInOnePageShuffled() {
        DriverContext driverContext = driverContext();
        Page source = CannedSourceOperator.mergePages(
            CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), between(100, 5000)))
        );
        loadSimpleAndAssert(driverContext, List.of(shuffle(source)), Block.MvOrdering.UNORDERED, Block.MvOrdering.UNORDERED);
    }

    private Page shuffle(Page source) {
        try {
            List<Integer> shuffleList = new ArrayList<>();
            IntStream.range(0, source.getPositionCount()).forEach(i -> shuffleList.add(i));
            Randomness.shuffle(shuffleList);
            int[] shuffleArray = shuffleList.stream().mapToInt(Integer::intValue).toArray();
            Block[] shuffledBlocks = new Block[source.getBlockCount()];
            for (int b = 0; b < shuffledBlocks.length; b++) {
                shuffledBlocks[b] = source.getBlock(b).filter(shuffleArray);
            }
            return new Page(shuffledBlocks);
        } finally {
            source.releaseBlocks();
        }
    }

    private static ValuesSourceReaderOperator.FieldInfo fieldInfo(MappedFieldType ft, ElementType elementType) {
        return new ValuesSourceReaderOperator.FieldInfo(ft.name(), elementType, shardIdx -> {
            if (shardIdx != 0) {
                fail("unexpected shardIdx [" + shardIdx + "]");
            }
            return ft.blockLoader(blContext());
        });
    }

    public static MappedFieldType.BlockLoaderContext blContext() {
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

        List<Operator> operators = new ArrayList<>();
        operators.add(
            new ValuesSourceReaderOperator.Factory(
                List.of(fieldInfo(mapperService.fieldType("key"), ElementType.INT)),
                List.of(
                    new ValuesSourceReaderOperator.ShardContext(
                        reader,
                        () -> SourceLoader.FROM_STORED_SOURCE,
                        STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                    )
                ),
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
                    b.stream().map(i -> i.info).toList(),
                    List.of(
                        new ValuesSourceReaderOperator.ShardContext(
                            reader,
                            () -> SourceLoader.FROM_STORED_SOURCE,
                            STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                        )
                    ),
                    0
                ).get(driverContext)
            );
        }
        List<Page> results = drive(operators, input.iterator(), driverContext);
        assertThat(results, hasSize(input.size()));
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(tests.size() + 2 /* one for doc and one for keys */));
            IntVector keys = page.<IntBlock>getBlock(1).asVector();
            for (int p = 0; p < page.getPositionCount(); p++) {
                int key = keys.getInt(p);
                for (int i = 0; i < tests.size(); i++) {
                    try {
                        tests.get(i).checkResults.check(page.getBlock(2 + i), p, key);
                    } catch (AssertionError e) {
                        throw new AssertionError("error checking " + tests.get(i).info.name() + "[" + p + "]: " + e.getMessage(), e);
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

    interface CheckResults {
        void check(Block block, int position, int key);
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
        assertThat(reader.leaves(), hasSize(10));
        assertThat(input, hasSize(10));
        List<FieldCase> cases = infoAndChecksForEachType(
            Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING,
            Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
        );
        // Build one operator for each field, so we get a unique map to assert on
        List<Operator> operators = cases.stream()
            .map(
                i -> new ValuesSourceReaderOperator.Factory(
                    List.of(i.info),
                    List.of(
                        new ValuesSourceReaderOperator.ShardContext(
                            reader,
                            () -> SourceLoader.FROM_STORED_SOURCE,
                            STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                        )
                    ),
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
            fc.checkReaders.check(fc.info.name(), allInOnePage, input.size(), reader.leaves().size(), status.readersBuilt());
        }
    }

    private List<FieldCase> infoAndChecksForEachType(
        Block.MvOrdering booleanAndNumericalDocValuesMvOrdering,
        Block.MvOrdering bytesRefDocValuesMvOrdering
    ) {
        Checks checks = new Checks(booleanAndNumericalDocValuesMvOrdering, bytesRefDocValuesMvOrdering);
        List<FieldCase> r = new ArrayList<>();
        r.add(new FieldCase(mapperService.fieldType(IdFieldMapper.NAME), ElementType.BYTES_REF, checks::ids, StatusChecks::id));
        r.add(new FieldCase(TsidExtractingIdFieldMapper.INSTANCE.fieldType(), ElementType.BYTES_REF, checks::ids, StatusChecks::id));
        r.add(new FieldCase(mapperService.fieldType("long"), ElementType.LONG, checks::longs, StatusChecks::longsFromDocValues));
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
        r.add(new FieldCase(mapperService.fieldType("mv_short"), ElementType.INT, checks::mvShorts, StatusChecks::mvShortsFromDocValues));
        r.add(new FieldCase(mapperService.fieldType("missing_short"), ElementType.INT, checks::constantNulls, StatusChecks::constantNulls));
        r.add(new FieldCase(mapperService.fieldType("byte"), ElementType.INT, checks::bytes, StatusChecks::bytesFromDocValues));
        r.add(new FieldCase(mapperService.fieldType("mv_byte"), ElementType.INT, checks::mvBytes, StatusChecks::mvBytesFromDocValues));
        r.add(new FieldCase(mapperService.fieldType("missing_byte"), ElementType.INT, checks::constantNulls, StatusChecks::constantNulls));
        r.add(new FieldCase(mapperService.fieldType("double"), ElementType.DOUBLE, checks::doubles, StatusChecks::doublesFromDocValues));
        r.add(
            new FieldCase(mapperService.fieldType("mv_double"), ElementType.DOUBLE, checks::mvDoubles, StatusChecks::mvDoublesFromDocValues)
        );
        r.add(
            new FieldCase(mapperService.fieldType("missing_double"), ElementType.DOUBLE, checks::constantNulls, StatusChecks::constantNulls)
        );
        r.add(new FieldCase(mapperService.fieldType("bool"), ElementType.BOOLEAN, checks::bools, StatusChecks::boolFromDocValues));
        r.add(new FieldCase(mapperService.fieldType("mv_bool"), ElementType.BOOLEAN, checks::mvBools, StatusChecks::mvBoolFromDocValues));
        r.add(
            new FieldCase(mapperService.fieldType("missing_bool"), ElementType.BOOLEAN, checks::constantNulls, StatusChecks::constantNulls)
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
        r.add(new FieldCase(mapperService.fieldType("source_text"), ElementType.BYTES_REF, checks::strings, StatusChecks::textFromSource));
        r.add(
            new FieldCase(
                mapperService.fieldType("mv_source_text"),
                ElementType.BYTES_REF,
                checks::mvStringsUnordered,
                StatusChecks::mvTextFromSource
            )
        );
        r.add(new FieldCase(storedTextField("stored_text"), ElementType.BYTES_REF, checks::strings, StatusChecks::textFromStored));
        r.add(
            new FieldCase(
                storedTextField("mv_stored_text"),
                ElementType.BYTES_REF,
                checks::mvStringsUnordered,
                StatusChecks::mvTextFromStored
            )
        );
        r.add(
            new FieldCase(
                textFieldWithDelegate("text_with_delegate", new KeywordFieldMapper.KeywordFieldType("kwd")),
                ElementType.BYTES_REF,
                checks::tags,
                StatusChecks::textWithDelegate
            )
        );
        r.add(
            new FieldCase(
                textFieldWithDelegate("mv_text_with_delegate", new KeywordFieldMapper.KeywordFieldType("mv_kwd")),
                ElementType.BYTES_REF,
                checks::mvStringsFromDocValues,
                StatusChecks::mvTextWithDelegate
            )
        );
        r.add(
            new FieldCase(
                textFieldWithDelegate("missing_text_with_delegate", new KeywordFieldMapper.KeywordFieldType("missing_kwd")),
                ElementType.BYTES_REF,
                checks::constantNulls,
                StatusChecks::constantNullTextWithDelegate
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
        Collections.shuffle(r, random());
        return r;
    }

    public void testLoadLong() throws IOException {
        testLoadLong(false, false);
    }

    public void testLoadLongManySegments() throws IOException {
        testLoadLong(false, true);
    }

    public void testLoadLongShuffled() throws IOException {
        testLoadLong(true, false);
    }

    public void testLoadLongShuffledManySegments() throws IOException {
        testLoadLong(true, true);
    }

    private void testLoadLong(boolean shuffle, boolean manySegments) throws IOException {
        int numDocs = between(10, 500);
        initMapping();
        keyToTags.clear();
        reader = initIndexLongField(directory, numDocs, manySegments ? commitEvery(numDocs) : numDocs);

        DriverContext driverContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(sourceOperator(driverContext, numDocs));
        assertThat(reader.leaves(), hasSize(manySegments ? greaterThan(5) : equalTo(1)));
        assertThat(input, hasSize(reader.leaves().size()));
        if (manySegments) {
            input = List.of(CannedSourceOperator.mergePages(input));
        }
        if (shuffle) {
            input = input.stream().map(this::shuffle).toList();
        }

        Checks checks = new Checks(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING, Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);

        List<FieldCase> cases = List.of(
            new FieldCase(
                mapperService.fieldType("long_source_text"),
                ElementType.BYTES_REF,
                checks::strings,
                StatusChecks::longTextFromSource
            )
        );
        // Build one operator for each field, so we get a unique map to assert on
        List<Operator> operators = cases.stream()
            .map(
                i -> new ValuesSourceReaderOperator.Factory(
                    List.of(i.info),
                    List.of(
                        new ValuesSourceReaderOperator.ShardContext(
                            reader,
                            () -> SourceLoader.FROM_STORED_SOURCE,
                            STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                        )
                    ),
                    0
                ).get(driverContext)
            )
            .toList();
        drive(operators, input.iterator(), driverContext);
        for (int i = 0; i < cases.size(); i++) {
            ValuesSourceReaderOperatorStatus status = (ValuesSourceReaderOperatorStatus) operators.get(i).status();
            assertThat(status.pagesReceived(), equalTo(input.size()));
            assertThat(status.pagesEmitted(), equalTo(input.size()));
        }
    }

    record Checks(Block.MvOrdering booleanAndNumericalDocValuesMvOrdering, Block.MvOrdering bytesRefDocValuesMvOrdering) {
        void longs(Block block, int position, int key) {
            LongVector longs = ((LongBlock) block).asVector();
            assertThat(longs.getLong(position), equalTo((long) key));
        }

        void ints(Block block, int position, int key) {
            IntVector ints = ((IntBlock) block).asVector();
            assertThat(ints.getInt(position), equalTo(key));
        }

        void shorts(Block block, int position, int key) {
            IntVector ints = ((IntBlock) block).asVector();
            assertThat(ints.getInt(position), equalTo((int) (short) key));
        }

        void bytes(Block block, int position, int key) {
            IntVector ints = ((IntBlock) block).asVector();
            assertThat(ints.getInt(position), equalTo((int) (byte) key));
        }

        void doubles(Block block, int position, int key) {
            DoubleVector doubles = ((DoubleBlock) block).asVector();
            assertThat(doubles.getDouble(position), equalTo(key / 123_456d));
        }

        void strings(Block block, int position, int key) {
            BytesRefVector keywords = ((BytesRefBlock) block).asVector();
            assertThat(keywords.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo(Integer.toString(key)));
        }

        void tags(Block block, int position, int key) {
            BytesRefVector keywords = ((BytesRefBlock) block).asVector();
            assertThat(keywords.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo(keyToTags.get(key)));
        }

        void bools(Block block, int position, int key) {
            BooleanVector bools = ((BooleanBlock) block).asVector();
            assertThat(bools.getBoolean(position), equalTo(key % 2 == 0));
        }

        void ids(Block block, int position, int key) {
            BytesRefVector ids = ((BytesRefBlock) block).asVector();
            assertThat(ids.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo("id" + key));
        }

        void constantBytes(Block block, int position, int key) {
            BytesRefVector keywords = ((BytesRefBlock) block).asVector();
            assertThat(keywords.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo("foo"));
        }

        void constantNulls(Block block, int position, int key) {
            assertTrue(block.areAllValuesNull());
            assertTrue(block.isNull(position));
        }

        void mvLongsFromDocValues(Block block, int position, int key) {
            mvLongs(block, position, key, booleanAndNumericalDocValuesMvOrdering);
        }

        void mvLongsUnordered(Block block, int position, int key) {
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

        void mvIntsFromDocValues(Block block, int position, int key) {
            mvInts(block, position, key, booleanAndNumericalDocValuesMvOrdering);
        }

        void mvIntsUnordered(Block block, int position, int key) {
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

        void mvShorts(Block block, int position, int key) {
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

        void mvBytes(Block block, int position, int key) {
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

        void mvDoubles(Block block, int position, int key) {
            DoubleBlock doubles = (DoubleBlock) block;
            int offset = doubles.getFirstValueIndex(position);
            for (int v = 0; v <= key % 3; v++) {
                assertThat(doubles.getDouble(offset + v), equalTo(key / 123_456d + v));
            }
            if (key % 3 > 0) {
                assertThat(doubles.mvOrdering(), equalTo(booleanAndNumericalDocValuesMvOrdering));
            }
        }

        void mvStringsFromDocValues(Block block, int position, int key) {
            mvStrings(block, position, key, bytesRefDocValuesMvOrdering);
        }

        void mvStringsUnordered(Block block, int position, int key) {
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

        void mvBools(Block block, int position, int key) {
            BooleanBlock bools = (BooleanBlock) block;
            assertThat(bools.getValueCount(position), equalTo(key % 3 + 1));
            int offset = bools.getFirstValueIndex(position);
            for (int v = 0; v <= key % 3; v++) {
                assertThat(bools.getBoolean(offset + v), equalTo(BOOLEANS[key % 3][v]));
            }
            if (key % 3 > 0) {
                assertThat(bools.mvOrdering(), equalTo(booleanAndNumericalDocValuesMvOrdering));
            }
        }
    }

    class StatusChecks {
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

        static void boolFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("bool", "Booleans", forcedRowByRow, pageCount, segmentCount, readers);
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

        static void textFromSource(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("source_text", "Bytes", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void longTextFromSource(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("long_source_text", "Bytes", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void textFromStored(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            stored("stored_text", "Bytes", forcedRowByRow, pageCount, segmentCount, readers);
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

        static void mvBoolFromDocValues(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_bool", "Booleans", forcedRowByRow, pageCount, segmentCount, readers);
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

        static void mvTextFromStored(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            stored("mv_stored_text", "Bytes", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void mvTextFromSource(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("mv_source_text", "Bytes", forcedRowByRow, pageCount, segmentCount, readers);
        }

        static void textWithDelegate(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            if (forcedRowByRow) {
                assertMap(
                    readers,
                    matchesMap().entry(
                        "text_with_delegate:row_stride:Delegating[to=kwd, impl=BlockDocValuesReader.SingletonOrdinals]",
                        segmentCount
                    )
                );
            } else {
                assertMap(
                    readers,
                    matchesMap().entry(
                        "text_with_delegate:column_at_a_time:Delegating[to=kwd, impl=BlockDocValuesReader.SingletonOrdinals]",
                        lessThanOrEqualTo(pageCount)
                    )
                );
            }
        }

        static void mvTextWithDelegate(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            if (forcedRowByRow) {
                assertMap(
                    readers,
                    matchesMap().entry(
                        "mv_text_with_delegate:row_stride:Delegating[to=mv_kwd, impl=BlockDocValuesReader.Ordinals]",
                        equalTo(segmentCount)
                    )
                );
            } else {
                assertMap(
                    readers,
                    matchesMap().entry(
                        "mv_text_with_delegate:column_at_a_time:Delegating[to=mv_kwd, impl=BlockDocValuesReader.Ordinals]",
                        lessThanOrEqualTo(pageCount)
                    )
                );
            }
        }

        static void constantNullTextWithDelegate(boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            if (forcedRowByRow) {
                assertMap(
                    readers,
                    matchesMap().entry(
                        "missing_text_with_delegate:row_stride:Delegating[to=missing_kwd, impl=constant_nulls]",
                        segmentCount
                    )
                );
            } else {
                assertMap(
                    readers,
                    matchesMap().entry(
                        "missing_text_with_delegate:column_at_a_time:Delegating[to=missing_kwd, impl=constant_nulls]",
                        lessThanOrEqualTo(pageCount)
                    )
                );
            }
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
                    readers,
                    matchesMap().entry(name + ":row_stride:BlockDocValuesReader.Singleton" + type, lessThanOrEqualTo(segmentCount))
                );
            } else {
                assertMap(
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
        mapperService = new MapperServiceTestCase() {
        }.createMapperService(MapperServiceTestCase.mapping(b -> {
            fieldExamples(b, "i", "integer");
            fieldExamples(b, "j", "long");
            fieldExamples(b, "d", "double");
        }));
        MappedFieldType intFt = mapperService.fieldType("i");
        MappedFieldType longFt = mapperService.fieldType("j");
        MappedFieldType doubleFt = mapperService.fieldType("d");
        MappedFieldType kwFt = new KeywordFieldMapper.KeywordFieldType("kw");

        NumericDocValuesField intField = new NumericDocValuesField(intFt.name(), 0);
        NumericDocValuesField longField = new NumericDocValuesField(longFt.name(), 0);
        NumericDocValuesField doubleField = new DoubleDocValuesField(doubleFt.name(), 0);
        final int numDocs = between(100, 5000);
        try (RandomIndexWriter w = new RandomIndexWriter(random(), directory)) {
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
            reader = w.getReader();
        }

        DriverContext driverContext = driverContext();
        var luceneFactory = new LuceneSourceOperator.Factory(
            List.of(new LuceneSourceOperatorTests.MockShardContext(reader, 0)),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(new MatchAllDocsQuery(), List.of())),
            randomFrom(DataPartitioning.values()),
            randomIntBetween(1, 10),
            randomPageSize(),
            LuceneOperator.NO_LIMIT,
            false // no scoring
        );
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                luceneFactory.get(driverContext),
                List.of(
                    factory(reader, intFt, ElementType.INT).get(driverContext),
                    factory(reader, longFt, ElementType.LONG).get(driverContext),
                    factory(reader, doubleFt, ElementType.DOUBLE).get(driverContext),
                    factory(reader, kwFt, ElementType.BYTES_REF).get(driverContext)
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

    private TextFieldMapper.TextFieldType storedTextField(String name) {
        return new TextFieldMapper.TextFieldType(
            name,
            false,
            true,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            true, // TODO randomize - if the field is stored we should load from the stored field even if there is source
            null,
            Map.of(),
            false,
            false
        );
    }

    private TextFieldMapper.TextFieldType textFieldWithDelegate(String name, KeywordFieldMapper.KeywordFieldType delegate) {
        return new TextFieldMapper.TextFieldType(
            name,
            false,
            false,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            randomBoolean(),
            delegate,
            Map.of(),
            false,
            false
        );
    }

    public void testNullsShared() {
        DriverContext driverContext = driverContext();
        int[] pages = new int[] { 0 };
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                simpleInput(driverContext.blockFactory(), 10),
                List.of(
                    new ValuesSourceReaderOperator.Factory(
                        List.of(
                            new ValuesSourceReaderOperator.FieldInfo("null1", ElementType.NULL, shardIdx -> BlockLoader.CONSTANT_NULLS),
                            new ValuesSourceReaderOperator.FieldInfo("null2", ElementType.NULL, shardIdx -> BlockLoader.CONSTANT_NULLS)
                        ),
                        List.of(
                            new ValuesSourceReaderOperator.ShardContext(
                                reader,
                                () -> SourceLoader.FROM_STORED_SOURCE,
                                STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                            )
                        ),
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

    public void testSequentialStoredFieldsTooSmall() throws IOException {
        testSequentialStoredFields(false, between(1, ValuesFromSingleReader.SEQUENTIAL_BOUNDARY - 1));
    }

    public void testSequentialStoredFieldsBigEnough() throws IOException {
        testSequentialStoredFields(
            true,
            between(ValuesFromSingleReader.SEQUENTIAL_BOUNDARY, ValuesFromSingleReader.SEQUENTIAL_BOUNDARY * 2)
        );
    }

    private void testSequentialStoredFields(boolean sequential, int docCount) throws IOException {
        initMapping();
        DriverContext driverContext = driverContext();
        List<Page> source = CannedSourceOperator.collectPages(simpleInput(driverContext, docCount, docCount, docCount));
        assertThat(source, hasSize(1)); // We want one page for simpler assertions, and we want them all in one segment
        assertTrue(source.get(0).<DocBlock>getBlock(0).asVector().singleSegmentNonDecreasing());
        Operator op = new ValuesSourceReaderOperator.Factory(
            List.of(
                fieldInfo(mapperService.fieldType("key"), ElementType.INT),
                fieldInfo(storedTextField("stored_text"), ElementType.BYTES_REF)
            ),
            List.of(
                new ValuesSourceReaderOperator.ShardContext(
                    reader,
                    () -> SourceLoader.FROM_STORED_SOURCE,
                    STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                )
            ),
            0
        ).get(driverContext);
        List<Page> results = drive(op, source.iterator(), driverContext);
        Checks checks = new Checks(Block.MvOrdering.UNORDERED, Block.MvOrdering.UNORDERED);
        IntVector keys = results.get(0).<IntBlock>getBlock(1).asVector();
        for (int p = 0; p < results.get(0).getPositionCount(); p++) {
            int key = keys.getInt(p);
            checks.strings(results.get(0).getBlock(2), p, key);
        }
        ValuesSourceReaderOperatorStatus status = (ValuesSourceReaderOperatorStatus) op.status();
        assertMap(
            status.readersBuilt(),
            matchesMap().entry("key:column_at_a_time:BlockDocValuesReader.SingletonInts", 1)
                .entry("stored_text:column_at_a_time:null", 1)
                .entry("stored_text:row_stride:BlockStoredFieldsReader.Bytes", 1)
                .entry("stored_fields[requires_source:false, fields:1, sequential: " + sequential + "]", 1)
        );
        assertDriverContext(driverContext);
    }

    public void testDescriptionOfMany() throws IOException {
        initIndex(1, 1);
        Block.MvOrdering ordering = randomFrom(Block.MvOrdering.values());
        List<FieldCase> cases = infoAndChecksForEachType(ordering, ordering);

        ValuesSourceReaderOperator.Factory factory = new ValuesSourceReaderOperator.Factory(
            cases.stream().map(c -> c.info).toList(),
            List.of(
                new ValuesSourceReaderOperator.ShardContext(
                    reader,
                    () -> SourceLoader.FROM_STORED_SOURCE,
                    STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                )
            ),
            0
        );
        assertThat(factory.describe(), equalTo("ValuesSourceReaderOperator[fields = [" + cases.size() + " fields]]"));
        try (Operator op = factory.get(driverContext())) {
            assertThat(op.toString(), equalTo("ValuesSourceReaderOperator[fields = [" + cases.size() + " fields]]"));
        }
    }

    public void testManyShards() throws IOException {
        initMapping();
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
                closeMe[d * 2] = readers[d] = initIndex(dirs[d], size, between(10, size * 2));
            }
            List<ShardContext> contexts = new ArrayList<>();
            List<ValuesSourceReaderOperator.ShardContext> readerShardContexts = new ArrayList<>();
            for (int s = 0; s < shardCount; s++) {
                contexts.add(new LuceneSourceOperatorTests.MockShardContext(readers[s], s));
                readerShardContexts.add(
                    new ValuesSourceReaderOperator.ShardContext(
                        readers[s],
                        () -> SourceLoader.FROM_STORED_SOURCE,
                        STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                    )
                );
            }
            var luceneFactory = new LuceneSourceOperator.Factory(
                contexts,
                ctx -> List.of(new LuceneSliceQueue.QueryAndTags(new MatchAllDocsQuery(), List.of())),
                DataPartitioning.SHARD,
                randomIntBetween(1, 10),
                1000,
                LuceneOperator.NO_LIMIT,
                false // no scoring
            );
            MappedFieldType ft = mapperService.fieldType("key");
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
                    keyCounts.merge(keys.getInt(i), 1, (prev, one) -> prev + one);
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
}
