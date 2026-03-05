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
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.mockfile.HandleLimitFS;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.lucene.query.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperatorTests;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.DummyBlockLoaderContext;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.blockloader.ConstantBytes;
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
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
        return new ValuesSourceReaderOperator.Factory(
            ByteSizeValue.ofGb(1),
            List.of(new ValuesSourceReaderOperator.FieldInfo(name, elementType, false, shardIdx -> {
                if (shardIdx != 0) {
                    fail("unexpected shardIdx [" + shardIdx + "]");
                }
                return ValuesSourceReaderOperator.load(loader);
            })),
            new IndexedByShardIdFromSingleton<>(
                new ValuesSourceReaderOperator.ShardContext(
                    reader,
                    (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE,
                    STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                )
            ),
            randomBoolean(),
            0
        );
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return simpleInput(driverContext(), size, commitEvery(size), randomPageSize());
    }

    private int commitEvery(int numDocs) {
        return Math.max(1, (int) Math.ceil((double) numDocs / 8));
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
            new IndexedByShardIdFromSingleton<>(new LuceneSourceOperatorTests.MockShardContext(reader, 0)),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())),
            DataPartitioning.SHARD,
            DataPartitioning.AutoStrategy.DEFAULT,
            randomIntBetween(1, 10),
            pageSize,
            LuceneOperator.NO_LIMIT,
            false // no scoring
        );
        return luceneFactory.get(context);
    }

    private void initMapping() throws IOException {
        mapperService = new MapperServiceTestCase() {}.createMapperService(MapperServiceTestCase.mapping(b -> {
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

    private IndexReader initIndexLongField(Directory directory, int size, int commitEvery, boolean forceMerge) throws IOException {
        try (
            IndexWriter writer = new IndexWriter(
                directory,
                newIndexWriterConfig().setMergePolicy(new TieredMergePolicy()).setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            )
        ) {
            for (int d = 0; d < size; d++) {
                XContentBuilder source = JsonXContent.contentBuilder();
                source.startObject();
                source.field("long_source_text", d + "#" + "a".repeat(100 * 1024));
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

            if (forceMerge) {
                writer.forceMerge(1);
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
        testManySingleDocPages(true);
    }

    public void testManySingleDocPagesNoReuse() {
        testManySingleDocPages(false);
    }

    private void testManySingleDocPages(boolean reuseColumnLoaders) {
        DriverContext driverContext = driverContext();

        boolean shuffle = randomBoolean();
        int sortedPages = 5;
        int numDocs = between(10, 100);
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext, numDocs, between(1, numDocs), 1));
        if (shuffle) {
            Randomness.shuffle(input.subList(sortedPages, input.size() - 1));  // Sort some of the list so we reuse some loaders
        }
        Checks checks = new Checks(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING, Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
        FieldCase testCase = fc(new KeywordFieldMapper.KeywordFieldType("kwd"), ElementType.BYTES_REF).results(checks::tags)
            .readers(StatusChecks::keywordsFromDocValues);
        Operator load = new ValuesSourceReaderOperator.Factory(
            ByteSizeValue.ofGb(1),
            List.of(testCase.info, fieldInfo(mapperService.fieldType("key"), ElementType.INT)),
            new IndexedByShardIdFromSingleton<>(
                new ValuesSourceReaderOperator.ShardContext(
                    reader,
                    (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE,
                    STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                )
            ),
            reuseColumnLoaders,
            0
        ).get(driverContext);
        List<Page> results = new TestDriverRunner().numThreads(1).builder(driverContext).input(input).run(load);
        assertThat(results, hasSize(input.size()));
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(3));
            IntVector keys = page.<IntBlock>getBlock(2).asVector();
            for (int p = 0; p < page.getPositionCount(); p++) {
                int key = keys.getInt(p);
                testCase.checkResults.check(page.getBlock(1), p, key, null);
            }
        }
        ValuesSourceReaderOperatorStatus status = (ValuesSourceReaderOperatorStatus) load.status();
        Matcher<Integer> readersMatcher;
        if (reuseColumnLoaders) {
            if (shuffle) {
                readersMatcher = lessThanOrEqualTo(numDocs - sortedPages + reader.leaves().size());
            } else {
                readersMatcher = equalTo(reader.leaves().size());
            }
        } else {
            readersMatcher = equalTo(numDocs);
        }
        assertMap(
            status.readersBuilt(),
            matchesMap().entry("key:column_at_a_time:IntsFromDocValues.Singleton", readersMatcher)
                .entry("kwd:column_at_a_time:BytesRefsFromOrds.Singleton", readersMatcher)
        );
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
                shuffledBlocks[b] = source.getBlock(b).filter(false, shuffleArray);
            }
            return new Page(shuffledBlocks);
        } finally {
            source.releaseBlocks();
        }
    }

    private static ValuesSourceReaderOperator.FieldInfo fieldInfo(MappedFieldType ft, ElementType elementType) {
        return new ValuesSourceReaderOperator.FieldInfo(ft.name(), elementType, false, shardIdx -> {
            if (shardIdx != 0) {
                fail("unexpected shardIdx [" + shardIdx + "]");
            }
            return ValuesSourceReaderOperator.load(ft.blockLoader(blContext()));
        });
    }

    public static MappedFieldType.BlockLoaderContext blContext() {
        return new DummyBlockLoaderContext("test_index") {
            @Override
            public IndexSettings indexSettings() {
                var imd = IndexMetadata.builder("test_index")
                    .settings(ValueSourceReaderTypeConversionTests.indexSettings(IndexVersion.current(), 1, 1).put(Settings.EMPTY))
                    .build();
                return new IndexSettings(imd, Settings.EMPTY);
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

            @Override
            public MappingLookup mappingLookup() {
                return null;
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
                ByteSizeValue.ofGb(1),
                List.of(fieldInfo(mapperService.fieldType("key"), ElementType.INT)),
                new IndexedByShardIdFromSingleton<>(
                    new ValuesSourceReaderOperator.ShardContext(
                        reader,
                        (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE,
                        STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                    )
                ),
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
                    b.stream().map(i -> i.info).toList(),
                    new IndexedByShardIdFromSingleton<>(
                        new ValuesSourceReaderOperator.ShardContext(
                            reader,
                            (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE,
                            STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                        )
                    ),
                    randomBoolean(),
                    0
                ).get(driverContext)
            );
        }
        List<Page> results = new TestDriverRunner().builder(driverContext).input(input).run(operators);
        assertThat(results, hasSize(input.size()));
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(tests.size() + 2 /* one for doc and one for keys */));
            IntVector keys = page.<IntBlock>getBlock(1).asVector();
            for (int p = 0; p < page.getPositionCount(); p++) {
                int key = keys.getInt(p);
                for (int i = 0; i < tests.size(); i++) {
                    try {
                        tests.get(i).checkResults.check(page.getBlock(2 + i), p, key, null);
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

    interface CheckResultsWithIndexKey {
        void check(Block block, int position, int key, String indexKey);
    }

    interface CheckReaders {
        void check(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readersBuilt);
    }

    interface CheckReadersWithName {
        void check(String name, boolean reuseBlockLoaders, int pageCount, int segmentCount, Map<?, ?> readersBuilt);
    }

    static class FieldCase {
        private final ValuesSourceReaderOperator.FieldInfo info;
        CheckResultsWithIndexKey checkResults;
        CheckReadersWithName checkReaders;

        FieldCase(ValuesSourceReaderOperator.FieldInfo info) {
            this.info = info;
        }

        public ValuesSourceReaderOperator.FieldInfo info() {
            return info;
        }

        CheckResultsWithIndexKey results() {
            return checkResults;
        }

        FieldCase results(CheckResultsWithIndexKey checkResults) {
            this.checkResults = checkResults;
            return this;
        }

        FieldCase results(CheckResults checkResults) {
            return results((block, position, key, indexKey) -> checkResults.check(block, position, key));
        }

        CheckReadersWithName readers() {
            return checkReaders;
        }

        FieldCase readers(CheckReadersWithName checkReaders) {
            this.checkReaders = checkReaders;
            return this;
        }

        FieldCase readers(CheckReaders checkReaders) {
            return readers(
                (name, reuseBlockLoaders, pageCount, segmentCount, readersBuilt) -> checkReaders.check(
                    reuseBlockLoaders,
                    pageCount,
                    segmentCount,
                    readersBuilt
                )
            );
        }
    }

    private FieldCase fc(String name, ElementType elementType) {
        return fc(mapperService.fieldType(name), elementType);
    }

    private static FieldCase fc(MappedFieldType ft, ElementType elementType) {
        return new FieldCase(fieldInfo(ft, elementType));
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
        var runner = new TestDriverRunner().builder(driverContext());
        int numDocs = between(100, 5000);
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(runner.context(), numDocs, commitEvery(numDocs), numDocs));
        assertThat(reader.leaves(), hasSize(8));
        assertThat(input, hasSize(8));
        List<FieldCase> cases = infoAndChecksForEachType(
            Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING,
            Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
        );
        boolean reuseBlockLoaders = randomBoolean();
        if (allInOnePage) {
            input = List.of(CannedSourceOperator.mergePages(input));
        }
        runner.input(input);

        runner.run(
            // Build one operator for each field, so we get a unique map to assert on
            cases.stream()
                .map(
                    i -> new ValuesSourceReaderOperator.Factory(
                        ByteSizeValue.ofGb(1),
                        List.of(i.info),
                        new IndexedByShardIdFromSingleton<>(
                            new ValuesSourceReaderOperator.ShardContext(
                                reader,
                                (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE,
                                STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                            )
                        ),
                        reuseBlockLoaders,
                        0
                    ).get(runner.context())
                )
                .toList()
        );
        for (int i = 0; i < cases.size(); i++) {
            ValuesSourceReaderOperatorStatus status = (ValuesSourceReaderOperatorStatus) runner.statuses().get(i);
            assertThat(status.pagesReceived(), equalTo(input.size()));
            assertThat(status.pagesEmitted(), equalTo(input.size()));
            FieldCase fc = cases.get(i);
            fc.checkReaders.check(fc.info.name(), reuseBlockLoaders, input.size(), reader.leaves().size(), status.readersBuilt());
        }
    }

    private List<FieldCase> infoAndChecksForEachType(
        Block.MvOrdering booleanAndNumericalDocValuesMvOrdering,
        Block.MvOrdering bytesRefDocValuesMvOrdering
    ) {
        Checks checks = new Checks(booleanAndNumericalDocValuesMvOrdering, bytesRefDocValuesMvOrdering);
        List<FieldCase> r = new ArrayList<>();
        r.add(fc(IdFieldMapper.NAME, ElementType.BYTES_REF).results(checks::ids).readers(StatusChecks::id));
        r.add(fc(TsidExtractingIdFieldMapper.INSTANCE.fieldType(), ElementType.BYTES_REF).results(checks::ids).readers(StatusChecks::id));
        r.add(fc("long", ElementType.LONG).results(checks::longs).readers(StatusChecks::longsFromDocValues));
        r.add(fc("mv_long", ElementType.LONG).results(checks::mvLongsFromDocValues).readers(StatusChecks::mvLongsFromDocValues));
        r.add(fc("missing_long", ElementType.LONG).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(fc("source_long", ElementType.LONG).results(checks::longs).readers(StatusChecks::longsFromSource));
        r.add(fc("mv_source_long", ElementType.LONG).results(checks::mvLongsUnordered).readers(StatusChecks::mvLongsFromSource));
        r.add(fc("int", ElementType.INT).results(checks::ints).readers(StatusChecks::intsFromDocValues));
        r.add(fc("mv_int", ElementType.INT).results(checks::mvIntsFromDocValues).readers(StatusChecks::mvIntsFromDocValues));
        r.add(fc("missing_int", ElementType.INT).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(fc("source_int", ElementType.INT).results(checks::ints).readers(StatusChecks::intsFromSource));
        r.add(fc("mv_source_int", ElementType.INT).results(checks::mvIntsUnordered).readers(StatusChecks::mvIntsFromSource));
        r.add(fc("short", ElementType.INT).results(checks::shorts).readers(StatusChecks::shortsFromDocValues));
        r.add(fc("mv_short", ElementType.INT).results(checks::mvShorts).readers(StatusChecks::mvShortsFromDocValues));
        r.add(fc("missing_short", ElementType.INT).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(fc("byte", ElementType.INT).results(checks::bytes).readers(StatusChecks::bytesFromDocValues));
        r.add(fc("mv_byte", ElementType.INT).results(checks::mvBytes).readers(StatusChecks::mvBytesFromDocValues));
        r.add(fc("missing_byte", ElementType.INT).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(fc("double", ElementType.DOUBLE).results(checks::doubles).readers(StatusChecks::doublesFromDocValues));
        r.add(fc("mv_double", ElementType.DOUBLE).results(checks::mvDoubles).readers(StatusChecks::mvDoublesFromDocValues));
        r.add(fc("missing_double", ElementType.DOUBLE).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(fc("bool", ElementType.BOOLEAN).results(checks::bools).readers(StatusChecks::boolFromDocValues));
        r.add(fc("mv_bool", ElementType.BOOLEAN).results(checks::mvBools).readers(StatusChecks::mvBoolFromDocValues));
        r.add(fc("missing_bool", ElementType.BOOLEAN).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(fc("kwd", ElementType.BYTES_REF).results(checks::tags).readers(StatusChecks::keywordsFromDocValues));
        r.add(fc("mv_kwd", ElementType.BYTES_REF).results(checks::mvStringsFromDocValues).readers(StatusChecks::mvKeywordsFromDocValues));
        r.add(fc("missing_kwd", ElementType.BYTES_REF).results(checks::constantNulls).readers(StatusChecks::constantNulls));
        r.add(
            fc(storedKeywordField("stored_kwd"), ElementType.BYTES_REF).results(checks::strings).readers(StatusChecks::keywordsFromStored)
        );
        r.add(
            fc(storedKeywordField("mv_stored_kwd"), ElementType.BYTES_REF).results(checks::mvStringsUnordered)
                .readers(StatusChecks::mvKeywordsFromStored)
        );
        r.add(fc("source_kwd", ElementType.BYTES_REF).results(checks::strings).readers(StatusChecks::keywordsFromSource));
        r.add(fc("mv_source_kwd", ElementType.BYTES_REF).results(checks::mvStringsUnordered).readers(StatusChecks::mvKeywordsFromSource));
        r.add(fc("source_text", ElementType.BYTES_REF).results(checks::strings).readers(StatusChecks::textFromSource));
        r.add(fc("mv_source_text", ElementType.BYTES_REF).results(checks::mvStringsUnordered).readers(StatusChecks::mvTextFromSource));
        r.add(fc(storedTextField("stored_text"), ElementType.BYTES_REF).results(checks::strings).readers(StatusChecks::textFromStored));
        r.add(
            fc(storedTextField("mv_stored_text"), ElementType.BYTES_REF).results(checks::mvStringsUnordered)
                .readers(StatusChecks::mvTextFromStored)
        );
        r.add(
            fc(textFieldWithDelegate("text_with_delegate", new KeywordFieldMapper.KeywordFieldType("kwd")), ElementType.BYTES_REF).results(
                checks::tags
            ).readers(StatusChecks::textWithDelegate)
        );
        r.add(
            fc(textFieldWithDelegate("mv_text_with_delegate", new KeywordFieldMapper.KeywordFieldType("mv_kwd")), ElementType.BYTES_REF)
                .results(checks::mvStringsFromDocValues)
                .readers(StatusChecks::mvTextWithDelegate)
        );
        r.add(
            fc(
                textFieldWithDelegate("missing_text_with_delegate", new KeywordFieldMapper.KeywordFieldType("missing_kwd")),
                ElementType.BYTES_REF
            ).results(checks::constantNulls).readers(StatusChecks::constantNullTextWithDelegate)
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
        reader = initIndexLongField(directory, numDocs, manySegments ? commitEvery(numDocs) : numDocs, manySegments == false);

        var runner = new TestDriverRunner().builder(driverContext());
        List<Page> input = CannedSourceOperator.collectPages(sourceOperator(runner.context(), numDocs));
        assertThat(reader.leaves(), hasSize(manySegments ? greaterThan(1) : equalTo(1)));
        assertThat(input, hasSize(reader.leaves().size()));
        if (manySegments) {
            input = List.of(CannedSourceOperator.mergePages(input));
        }
        if (shuffle) {
            input = input.stream().map(this::shuffle).toList();
        }
        runner.input(input);
        boolean willSplit = loadLongWillSplit(input);

        Checks checks = new Checks(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING, Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);

        List<FieldCase> cases = List.of(
            fc("long_source_text", ElementType.BYTES_REF).results(checks::strings).readers(StatusChecks::longTextFromSource)
        );
        // Build one operator for each field, so we get a unique map to assert on
        List<Page> result = runner.run(
            cases.stream()
                .map(
                    i -> new ValuesSourceReaderOperator.Factory(
                        ByteSizeValue.ofGb(1),
                        List.of(i.info),
                        new IndexedByShardIdFromSingleton<>(
                            new ValuesSourceReaderOperator.ShardContext(
                                reader,
                                (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE,
                                STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                            )
                        ),
                        randomBoolean(),
                        0
                    ).get(runner.context())
                )
                .toList()
        );

        boolean[] found = new boolean[numDocs];
        for (Page page : result) {
            BytesRefVector bytes = page.<BytesRefBlock>getBlock(1).asVector();
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < bytes.getPositionCount(); p++) {
                BytesRef v = bytes.getBytesRef(p, scratch);
                int d = Integer.valueOf(v.utf8ToString().split("#")[0]);
                assertFalse("found a duplicate " + d, found[d]);
                found[d] = true;
            }
        }
        List<Integer> missing = new ArrayList<>();
        for (int d = 0; d < numDocs; d++) {
            if (found[d] == false) {
                missing.add(d);
            }
        }
        assertThat(missing, hasSize(0));
        assertThat(result, hasSize(willSplit ? greaterThanOrEqualTo(input.size()) : equalTo(input.size())));

        for (int i = 0; i < cases.size(); i++) {
            ValuesSourceReaderOperatorStatus status = (ValuesSourceReaderOperatorStatus) runner.statuses().get(i);
            assertThat(status.pagesReceived(), equalTo(input.size()));
            assertThat(status.pagesEmitted(), willSplit ? greaterThanOrEqualTo(input.size()) : equalTo(input.size()));
        }
    }

    private boolean loadLongWillSplit(List<Page> input) {
        int nextDoc = -1;
        for (Page page : input) {
            DocVector doc = page.<DocBlock>getBlock(0).asVector();
            for (int p = 0; p < doc.getPositionCount(); p++) {
                if (doc.shards().getInt(p) != 0) {
                    return false;
                }
                if (doc.segments().getInt(p) != 0) {
                    return false;
                }
                if (nextDoc == -1) {
                    nextDoc = doc.docs().getInt(p);
                } else if (doc.docs().getInt(p) != nextDoc) {
                    return false;
                }
                nextDoc++;
            }
        }
        return true;
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
        static void longsFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("long", "Longs", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void longsFromSource(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("source_long", "Longs", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void intsFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("int", "Ints", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void intsFromSource(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("source_int", "Ints", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void shortsFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("short", "Ints", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void bytesFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("byte", "Ints", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void doublesFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("double", "Doubles", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void boolFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("bool", "Booleans", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void keywordsFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues("kwd", "Ordinals", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void strFromDocValues(String name, boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            docValues(name, "Ordinals", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void keywordsFromStored(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            stored("stored_kwd", "Bytes", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void keywordsFromSource(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("source_kwd", "Bytes", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void textFromSource(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("source_text", "Bytes", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void longTextFromSource(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("long_source_text", "Bytes", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void textFromStored(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            stored("stored_text", "Bytes", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvLongsFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_long", "Longs", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvLongsFromSource(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("mv_source_long", "Longs", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvIntsFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_int", "Ints", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvIntsFromSource(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("mv_source_int", "Ints", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvShortsFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_short", "Ints", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvBytesFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_byte", "Ints", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvDoublesFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_double", "Doubles", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvBoolFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_bool", "Booleans", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvKeywordsFromDocValues(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            mvDocValues("mv_kwd", "Ordinals", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvKeywordsFromStored(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            stored("mv_stored_kwd", "Bytes", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvKeywordsFromSource(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("mv_source_kwd", "Bytes", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvTextFromStored(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            stored("mv_stored_text", "Bytes", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void mvTextFromSource(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            source("mv_source_text", "Bytes", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        static void textWithDelegate(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            assertMap(
                readers,
                matchesMap().entry(
                    "text_with_delegate:column_at_a_time:Delegating[to=kwd, impl=BytesRefsFromOrds.Singleton]",
                    countMatcher(reuseColumnLoaders, pageCount, segmentCount, 0)
                )
            );
        }

        static void mvTextWithDelegate(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            assertMap(
                readers,
                matchesMap().entry(
                    "mv_text_with_delegate:column_at_a_time:Delegating[to=mv_kwd, impl=BytesRefsFromOrds.SortedSet]",
                    countMatcher(reuseColumnLoaders, pageCount, segmentCount, 0)
                )
            );
        }

        static void constantNullTextWithDelegate(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            assertMap(
                readers,
                matchesMap().entry(
                    "missing_text_with_delegate:column_at_a_time:Delegating[to=missing_kwd, impl=constant_nulls]",
                    countMatcher(reuseColumnLoaders, pageCount, segmentCount, 0)
                )
            );
        }

        private static void docValues(
            String name,
            String type,
            boolean reuseColumnLoaders,
            int pageCount,
            int segmentCount,
            Map<?, ?> readers
        ) {
            assertMap(
                readers,
                matchesMap().entry(
                    name + ":column_at_a_time:" + singleName(type),
                    countMatcher(reuseColumnLoaders, pageCount, segmentCount, 0)
                )
            );
        }

        private static void mvDocValues(
            String name,
            String type,
            boolean reuseColumnLoaders,
            int pageCount,
            int segmentCount,
            Map<?, ?> readers
        ) {
            Integer singletons = (Integer) readers.remove(name + ":column_at_a_time:" + singleName(type));
            assertMap(
                readers,
                matchesMap().entry(
                    name + ":column_at_a_time:" + multiName(type),
                    countMatcher(reuseColumnLoaders, pageCount, segmentCount, singletons == null ? 0 : singletons)
                )
            );
        }

        static String singleName(String type) {
            return switch (type) {
                case "Ordinals" -> "BytesRefsFromOrds.Singleton";
                default -> type + "FromDocValues.Singleton";
            };
        }

        static String multiName(String type) {
            return switch (type) {
                case "Ordinals" -> "BytesRefsFromOrds.SortedSet";
                default -> type + "FromDocValues.Sorted";
            };
        }

        static void id(boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            stored("_id", "Id", reuseColumnLoaders, pageCount, segmentCount, readers);
        }

        private static void source(
            String name,
            String type,
            boolean reuseColumnLoaders,
            int pageCount,
            int segmentCount,
            Map<?, ?> readers
        ) {
            Integer columnAttempts = (Integer) readers.remove(name + ":column_at_a_time:null");
            assertThat(columnAttempts, not(nullValue()));

            Integer sequentialCount = (Integer) readers.remove("stored_fields[requires_source:true, fields:0, sequential: true]");
            Integer nonSequentialCount = (Integer) readers.remove("stored_fields[requires_source:true, fields:0, sequential: false]");
            int totalReaders = (sequentialCount == null ? 0 : sequentialCount) + (nonSequentialCount == null ? 0 : nonSequentialCount);
            assertThat(totalReaders, countMatcher(reuseColumnLoaders, pageCount, segmentCount, 0));

            assertMap(
                readers,
                matchesMap().entry(
                    name + ":row_stride:BlockSourceReader." + type,
                    countMatcher(reuseColumnLoaders, pageCount, segmentCount, 0)
                )
            );
        }

        private static Matcher<Integer> countMatcher(boolean reuseColumnLoaders, int pageCount, int segmentCount, int alreadyCounted) {
            if (pageCount == 1 || reuseColumnLoaders) {
                return equalTo(segmentCount - alreadyCounted);
            }
            return allOf(
                lessThanOrEqualTo(pageCount * segmentCount - alreadyCounted),
                greaterThanOrEqualTo(pageCount - alreadyCounted),
                greaterThanOrEqualTo(segmentCount - alreadyCounted)
            );
        }

        private static void stored(
            String name,
            String type,
            boolean reuseColumnLoaders,
            int pageCount,
            int segmentCount,
            Map<?, ?> readers
        ) {
            Integer columnAttempts = (Integer) readers.remove(name + ":column_at_a_time:null");
            assertThat(columnAttempts, not(nullValue()));

            Integer sequentialCount = (Integer) readers.remove("stored_fields[requires_source:false, fields:1, sequential: true]");
            Integer nonSequentialCount = (Integer) readers.remove("stored_fields[requires_source:false, fields:1, sequential: false]");
            int totalReaders = (sequentialCount == null ? 0 : sequentialCount) + (nonSequentialCount == null ? 0 : nonSequentialCount);
            assertThat(totalReaders, countMatcher(reuseColumnLoaders, pageCount, segmentCount, 0));

            assertMap(
                readers,
                matchesMap().entry(
                    name + ":row_stride:BlockStoredFieldsReader." + type,
                    countMatcher(reuseColumnLoaders, pageCount, segmentCount, 0)
                )
            );
        }

        static void constantBytes(String name, boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            assertMap(
                readers,
                matchesMap().entry(
                    name + ":column_at_a_time:constant[[66 6f 6f]]",
                    countMatcher(reuseColumnLoaders, pageCount, segmentCount, 0)
                )
            );
        }

        static void constantNulls(String name, boolean reuseColumnLoaders, int pageCount, int segmentCount, Map<?, ?> readers) {
            assertMap(
                readers,
                matchesMap().entry(name + ":column_at_a_time:constant_nulls", countMatcher(reuseColumnLoaders, pageCount, segmentCount, 0))
            );
        }

        static void unionFromDocValues(String name, boolean forcedRowByRow, int pageCount, int segmentCount, Map<?, ?> readers) {
            // TODO: develop a working check for this
            // docValues(name, "Ordinals", forcedRowByRow, pageCount, segmentCount, readers);
        }
    }

    public void testWithNulls() throws IOException {
        mapperService = new MapperServiceTestCase() {}.createMapperService(MapperServiceTestCase.mapping(b -> {
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
            new IndexedByShardIdFromSingleton<>(new LuceneSourceOperatorTests.MockShardContext(reader, 0)),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())),
            randomFrom(DataPartitioning.values()),
            DataPartitioning.AutoStrategy.DEFAULT,
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
            new TestDriverRunner().run(driver);
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
        return new KeywordFieldMapper.KeywordFieldType(name, ft, false);
    }

    private TextFieldMapper.TextFieldType storedTextField(String name) {
        return new TextFieldMapper.TextFieldType(
            name,
            false,
            true,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            true, // TODO randomize - if the field is stored we should load from the stored field even if there is source]
            false,
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
            false,
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
                        new IndexedByShardIdFromSingleton<>(
                            new ValuesSourceReaderOperator.ShardContext(
                                reader,
                                (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE,
                                STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                            )
                        ),
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
        var runner = new TestDriverRunner().builder(driverContext());
        List<Page> source = CannedSourceOperator.collectPages(simpleInput(runner.context(), docCount, docCount, docCount));
        assertThat(source, hasSize(1)); // We want one page for simpler assertions, and we want them all in one segment
        assertTrue(source.get(0).<DocBlock>getBlock(0).asVector().singleSegmentNonDecreasing());
        List<Page> results = runner.input(source)
            .run(
                new ValuesSourceReaderOperator.Factory(
                    ByteSizeValue.ofGb(1),
                    List.of(
                        fieldInfo(mapperService.fieldType("key"), ElementType.INT),
                        fieldInfo(storedTextField("stored_text"), ElementType.BYTES_REF)
                    ),
                    new IndexedByShardIdFromSingleton<>(
                        new ValuesSourceReaderOperator.ShardContext(
                            reader,
                            (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE,
                            STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                        )
                    ),
                    randomBoolean(),
                    0
                )
            );
        Checks checks = new Checks(Block.MvOrdering.UNORDERED, Block.MvOrdering.UNORDERED);
        IntVector keys = results.get(0).<IntBlock>getBlock(1).asVector();
        for (int p = 0; p < results.get(0).getPositionCount(); p++) {
            int key = keys.getInt(p);
            checks.strings(results.get(0).getBlock(2), p, key);
        }
        ValuesSourceReaderOperatorStatus status = (ValuesSourceReaderOperatorStatus) runner.statuses().getFirst();
        assertMap(
            status.readersBuilt(),
            matchesMap().entry("key:column_at_a_time:IntsFromDocValues.Singleton", 1)
                .entry("stored_text:column_at_a_time:null", 1)
                .entry("stored_text:row_stride:BlockStoredFieldsReader.Bytes", 1)
                .entry("stored_fields[requires_source:false, fields:1, sequential: " + sequential + "]", 1)
        );
        assertDriverContext(runner.context());
    }

    public void testDescriptionOfMany() throws IOException {
        initIndex(1, 1);
        Block.MvOrdering ordering = randomFrom(Block.MvOrdering.values());
        List<FieldCase> cases = infoAndChecksForEachType(ordering, ordering);

        ValuesSourceReaderOperator.Factory factory = new ValuesSourceReaderOperator.Factory(
            ByteSizeValue.ofGb(1),
            cases.stream().map(c -> c.info).toList(),
            new IndexedByShardIdFromSingleton<>(
                new ValuesSourceReaderOperator.ShardContext(
                    reader,
                    (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE,
                    STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                )
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
                        (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE,
                        STORED_FIELDS_SEQUENTIAL_PROPORTIONS
                    )
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
            MappedFieldType ft = mapperService.fieldType("key");
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
            var runner = new TestDriverRunner().builder(driverContext());
            List<Page> results = runner.input(luceneFactory).run(readerFactory);
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
