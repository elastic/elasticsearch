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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.mockfile.HandleLimitFS;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CannedSourceOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ProvidedIdFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.lucene.LuceneSourceOperatorTests.mockSearchContext;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

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

    private Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        if (reader == null) {
            // Init a reader if one hasn't been built, so things don't blow up
            try {
                initIndex(100);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return factory(reader, docValuesNumberField("long", NumberFieldMapper.NumberType.LONG));
    }

    static Operator.OperatorFactory factory(IndexReader reader, MappedFieldType ft) {
        return factory(reader, ft.name(), ft.blockLoader(null));
    }

    static Operator.OperatorFactory factory(IndexReader reader, String name, BlockLoader loader) {
        return new ValuesSourceReaderOperator.Factory(
            List.of(new ValuesSourceReaderOperator.FieldInfo(name, List.of(loader))),
            List.of(new ValuesSourceReaderOperator.ShardContext(reader, () -> SourceLoader.FROM_STORED_SOURCE)),
            0
        );
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        try {
            initIndex(size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        var luceneFactory = new LuceneSourceOperator.Factory(
            List.of(mockSearchContext(reader)),
            ctx -> new MatchAllDocsQuery(),
            randomFrom(DataPartitioning.values()),
            randomIntBetween(1, 10),
            randomPageSize(),
            LuceneOperator.NO_LIMIT
        );
        return luceneFactory.get(driverContext());
    }

    private void initIndex(int size) throws IOException {
        // The test wants more than one segment. We shoot for about 10.
        int commitEvery = Math.max(1, size / 10);
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int d = 0; d < size; d++) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(IdFieldMapper.standardIdField("id"));
                doc.add(new SortedNumericDocValuesField("key", d));
                doc.add(new SortedNumericDocValuesField("int", d));
                doc.add(new SortedNumericDocValuesField("short", (short) d));
                doc.add(new SortedNumericDocValuesField("byte", (byte) d));
                doc.add(new SortedNumericDocValuesField("long", d));
                doc.add(
                    new KeywordFieldMapper.KeywordField("kwd", new BytesRef(Integer.toString(d)), KeywordFieldMapper.Defaults.FIELD_TYPE)
                );
                doc.add(new StoredField("stored_kwd", new BytesRef(Integer.toString(d))));
                doc.add(new StoredField("stored_text", Integer.toString(d)));
                doc.add(new SortedNumericDocValuesField("bool", d % 2 == 0 ? 1 : 0));
                doc.add(new SortedNumericDocValuesField("double", NumericUtils.doubleToSortableLong(d / 123_456d)));
                for (int v = 0; v <= d % 3; v++) {
                    doc.add(new SortedNumericDocValuesField("mv_bool", v % 2 == 0 ? 1 : 0));
                    doc.add(new SortedNumericDocValuesField("mv_int", 1_000 * d + v));
                    doc.add(new SortedNumericDocValuesField("mv_short", (short) (2_000 * d + v)));
                    doc.add(new SortedNumericDocValuesField("mv_byte", (byte) (3_000 * d + v)));
                    doc.add(new SortedNumericDocValuesField("mv_long", -1_000 * d + v));
                    doc.add(new SortedNumericDocValuesField("mv_double", NumericUtils.doubleToSortableLong(d / 123_456d + v)));
                    doc.add(
                        new KeywordFieldMapper.KeywordField("mv_kwd", new BytesRef(PREFIX[v] + d), KeywordFieldMapper.Defaults.FIELD_TYPE)
                    );
                    doc.add(new StoredField("mv_stored_kwd", new BytesRef(PREFIX[v] + d)));
                    doc.add(new StoredField("mv_stored_text", PREFIX[v] + d));
                }
                XContentBuilder source = JsonXContent.contentBuilder();
                source.startObject();
                source.field("source_kwd", Integer.toString(d));
                source.startArray("mv_source_kwd");
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
                source.field("source_long", (long) d);
                source.startArray("mv_source_long");
                for (int v = 0; v <= d % 3; v++) {
                    source.value((long) (-1_000 * d + v));
                }
                source.endArray();
                source.field("source_int", d);
                source.startArray("mv_source_int");
                for (int v = 0; v <= d % 3; v++) {
                    source.value(1_000 * d + v);
                }
                source.endArray();

                source.endObject();
                doc.add(new StoredField(SourceFieldMapper.NAME, BytesReference.bytes(source).toBytesRef()));
                writer.addDocument(doc);
                if (d % commitEvery == 0) {
                    writer.commit();
                }
            }
            reader = writer.getReader();
        }
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "ValuesSourceReaderOperator[field = long]";
    }

    @Override
    protected String expectedToStringOfSimple() {
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
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeTrue("doesn't use big arrays so can't break", false);
        return null;
    }

    public void testLoadAll() {
        DriverContext driverContext = driverContext();
        loadSimpleAndAssert(
            driverContext,
            CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), between(100, 5000))),
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
            Block.MvOrdering.UNORDERED
        );
    }

    public void testEmpty() {
        DriverContext driverContext = driverContext();
        loadSimpleAndAssert(
            driverContext,
            CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), 0)),
            Block.MvOrdering.UNORDERED
        );
    }

    public void testLoadAllInOnePageShuffled() {
        DriverContext driverContext = driverContext();
        Page source = CannedSourceOperator.mergePages(
            CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), between(100, 5000)))
        );
        List<Integer> shuffleList = new ArrayList<>();
        IntStream.range(0, source.getPositionCount()).forEach(i -> shuffleList.add(i));
        Randomness.shuffle(shuffleList);
        int[] shuffleArray = shuffleList.stream().mapToInt(Integer::intValue).toArray();
        Block[] shuffledBlocks = new Block[source.getBlockCount()];
        for (int b = 0; b < shuffledBlocks.length; b++) {
            shuffledBlocks[b] = source.getBlock(b).filter(shuffleArray);
        }
        source = new Page(shuffledBlocks);
        loadSimpleAndAssert(driverContext, List.of(source), Block.MvOrdering.UNORDERED);
    }

    private static ValuesSourceReaderOperator.FieldInfo fieldInfo(MappedFieldType ft) {
        return new ValuesSourceReaderOperator.FieldInfo(ft.name(), List.of(ft.blockLoader(new MappedFieldType.BlockLoaderContext() {
            @Override
            public String indexName() {
                return "test_index";
            }

            @Override
            public SearchLookup lookup() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<String> sourcePaths(String name) {
                return Set.of(name);
            }
        })));
    }

    private void loadSimpleAndAssert(DriverContext driverContext, List<Page> input, Block.MvOrdering docValuesMvOrdering) {
        List<FieldCase> cases = infoAndChecksForEachType(docValuesMvOrdering);

        List<Operator> operators = new ArrayList<>();
        operators.add(
            new ValuesSourceReaderOperator.Factory(
                List.of(fieldInfo(docValuesNumberField("key", NumberFieldMapper.NumberType.INTEGER))),
                List.of(new ValuesSourceReaderOperator.ShardContext(reader, () -> SourceLoader.FROM_STORED_SOURCE)),
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
                    List.of(new ValuesSourceReaderOperator.ShardContext(reader, () -> SourceLoader.FROM_STORED_SOURCE)),
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
            assertThat(((ValuesSourceReaderOperator) op).status().pagesProcessed(), equalTo(input.size()));
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
        FieldCase(MappedFieldType ft, CheckResults checkResults, CheckReadersWithName checkReaders) {
            this(fieldInfo(ft), checkResults, checkReaders);
        }

        FieldCase(MappedFieldType ft, CheckResults checkResults, CheckReaders checkReaders) {
            this(
                ft,
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
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), between(100, 5000)));
        List<FieldCase> cases = infoAndChecksForEachType(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
        // Build one operator for each field, so we get a unique map to assert on
        List<Operator> operators = cases.stream()
            .map(
                i -> new ValuesSourceReaderOperator.Factory(
                    List.of(i.info),
                    List.of(new ValuesSourceReaderOperator.ShardContext(reader, () -> SourceLoader.FROM_STORED_SOURCE)),
                    0
                ).get(driverContext)
            )
            .toList();
        if (allInOnePage) {
            input = List.of(CannedSourceOperator.mergePages(input));
        }
        drive(operators, input.iterator(), driverContext);
        for (int i = 0; i < cases.size(); i++) {
            ValuesSourceReaderOperator.Status status = (ValuesSourceReaderOperator.Status) operators.get(i).status();
            assertThat(status.pagesProcessed(), equalTo(input.size()));
            FieldCase fc = cases.get(i);
            fc.checkReaders.check(fc.info.name(), allInOnePage, input.size(), reader.leaves().size(), status.readersBuilt());
        }
    }

    private List<FieldCase> infoAndChecksForEachType(Block.MvOrdering docValuesMvOrdering) {
        Checks checks = new Checks(docValuesMvOrdering);
        List<FieldCase> r = new ArrayList<>();
        r.add(
            new FieldCase(docValuesNumberField("long", NumberFieldMapper.NumberType.LONG), checks::longs, StatusChecks::longsFromDocValues)
        );
        r.add(
            new FieldCase(
                docValuesNumberField("mv_long", NumberFieldMapper.NumberType.LONG),
                checks::mvLongsFromDocValues,
                StatusChecks::mvLongsFromDocValues
            )
        );
        r.add(
            new FieldCase(
                docValuesNumberField("missing_long", NumberFieldMapper.NumberType.LONG),
                checks::constantNulls,
                StatusChecks::constantNulls
            )
        );
        r.add(
            new FieldCase(sourceNumberField("source_long", NumberFieldMapper.NumberType.LONG), checks::longs, StatusChecks::longsFromSource)
        );
        r.add(
            new FieldCase(
                sourceNumberField("mv_source_long", NumberFieldMapper.NumberType.LONG),
                checks::mvLongsUnordered,
                StatusChecks::mvLongsFromSource
            )
        );
        r.add(
            new FieldCase(docValuesNumberField("int", NumberFieldMapper.NumberType.INTEGER), checks::ints, StatusChecks::intsFromDocValues)
        );
        r.add(
            new FieldCase(
                docValuesNumberField("mv_int", NumberFieldMapper.NumberType.INTEGER),
                checks::mvIntsFromDocValues,
                StatusChecks::mvIntsFromDocValues
            )
        );
        r.add(
            new FieldCase(
                docValuesNumberField("missing_int", NumberFieldMapper.NumberType.INTEGER),
                checks::constantNulls,
                StatusChecks::constantNulls
            )
        );
        r.add(
            new FieldCase(sourceNumberField("source_int", NumberFieldMapper.NumberType.INTEGER), checks::ints, StatusChecks::intsFromSource)
        );
        r.add(
            new FieldCase(
                sourceNumberField("mv_source_int", NumberFieldMapper.NumberType.INTEGER),
                checks::mvIntsUnordered,
                StatusChecks::mvIntsFromSource
            )
        );
        r.add(
            new FieldCase(
                docValuesNumberField("short", NumberFieldMapper.NumberType.SHORT),
                checks::shorts,
                StatusChecks::shortsFromDocValues
            )
        );
        r.add(
            new FieldCase(
                docValuesNumberField("mv_short", NumberFieldMapper.NumberType.SHORT),
                checks::mvShorts,
                StatusChecks::mvShortsFromDocValues
            )
        );
        r.add(
            new FieldCase(
                docValuesNumberField("missing_short", NumberFieldMapper.NumberType.SHORT),
                checks::constantNulls,
                StatusChecks::constantNulls
            )
        );
        r.add(
            new FieldCase(docValuesNumberField("byte", NumberFieldMapper.NumberType.BYTE), checks::bytes, StatusChecks::bytesFromDocValues)
        );
        r.add(
            new FieldCase(
                docValuesNumberField("mv_byte", NumberFieldMapper.NumberType.BYTE),
                checks::mvBytes,
                StatusChecks::mvBytesFromDocValues
            )
        );
        r.add(
            new FieldCase(
                docValuesNumberField("missing_byte", NumberFieldMapper.NumberType.BYTE),
                checks::constantNulls,
                StatusChecks::constantNulls
            )
        );
        r.add(
            new FieldCase(
                docValuesNumberField("double", NumberFieldMapper.NumberType.DOUBLE),
                checks::doubles,
                StatusChecks::doublesFromDocValues
            )
        );
        r.add(
            new FieldCase(
                docValuesNumberField("mv_double", NumberFieldMapper.NumberType.DOUBLE),
                checks::mvDoubles,
                StatusChecks::mvDoublesFromDocValues
            )
        );
        r.add(
            new FieldCase(
                docValuesNumberField("missing_double", NumberFieldMapper.NumberType.DOUBLE),
                checks::constantNulls,
                StatusChecks::constantNulls
            )
        );
        r.add(new FieldCase(new BooleanFieldMapper.BooleanFieldType("bool"), checks::bools, StatusChecks::boolFromDocValues));
        r.add(new FieldCase(new BooleanFieldMapper.BooleanFieldType("mv_bool"), checks::mvBools, StatusChecks::mvBoolFromDocValues));
        r.add(new FieldCase(new BooleanFieldMapper.BooleanFieldType("missing_bool"), checks::constantNulls, StatusChecks::constantNulls));
        r.add(new FieldCase(new KeywordFieldMapper.KeywordFieldType("kwd"), checks::strings, StatusChecks::keywordsFromDocValues));
        r.add(
            new FieldCase(
                new KeywordFieldMapper.KeywordFieldType("mv_kwd"),
                checks::mvStringsFromDocValues,
                StatusChecks::mvKeywordsFromDocValues
            )
        );
        r.add(new FieldCase(new KeywordFieldMapper.KeywordFieldType("missing_kwd"), checks::constantNulls, StatusChecks::constantNulls));
        r.add(new FieldCase(storedKeywordField("stored_kwd"), checks::strings, StatusChecks::keywordsFromStored));
        r.add(new FieldCase(storedKeywordField("mv_stored_kwd"), checks::mvStringsUnordered, StatusChecks::mvKeywordsFromStored));
        r.add(new FieldCase(sourceKeywordField("source_kwd"), checks::strings, StatusChecks::keywordsFromSource));
        r.add(new FieldCase(sourceKeywordField("mv_source_kwd"), checks::mvStringsUnordered, StatusChecks::mvKeywordsFromSource));
        r.add(new FieldCase(new TextFieldMapper.TextFieldType("source_text", false), checks::strings, StatusChecks::textFromSource));
        r.add(
            new FieldCase(
                new TextFieldMapper.TextFieldType("mv_source_text", false),
                checks::mvStringsUnordered,
                StatusChecks::mvTextFromSource
            )
        );
        r.add(new FieldCase(storedTextField("stored_text"), checks::strings, StatusChecks::textFromStored));
        r.add(new FieldCase(storedTextField("mv_stored_text"), checks::mvStringsUnordered, StatusChecks::mvTextFromStored));
        r.add(
            new FieldCase(
                textFieldWithDelegate("text_with_delegate", new KeywordFieldMapper.KeywordFieldType("kwd")),
                checks::strings,
                StatusChecks::textWithDelegate
            )
        );
        r.add(
            new FieldCase(
                textFieldWithDelegate("mv_text_with_delegate", new KeywordFieldMapper.KeywordFieldType("mv_kwd")),
                checks::mvStringsFromDocValues,
                StatusChecks::mvTextWithDelegate
            )
        );
        r.add(
            new FieldCase(
                textFieldWithDelegate("missing_text_with_delegate", new KeywordFieldMapper.KeywordFieldType("missing_kwd")),
                checks::constantNulls,
                StatusChecks::constantNullTextWithDelegate
            )
        );
        r.add(new FieldCase(new ProvidedIdFieldMapper(() -> false).fieldType(), checks::ids, StatusChecks::id));
        r.add(new FieldCase(TsidExtractingIdFieldMapper.INSTANCE.fieldType(), checks::ids, StatusChecks::id));
        r.add(
            new FieldCase(
                new ValuesSourceReaderOperator.FieldInfo("constant_bytes", List.of(BlockLoader.constantBytes(new BytesRef("foo")))),
                checks::constantBytes,
                StatusChecks::constantBytes
            )
        );
        r.add(
            new FieldCase(
                new ValuesSourceReaderOperator.FieldInfo("null", List.of(BlockLoader.CONSTANT_NULLS)),
                checks::constantNulls,
                StatusChecks::constantNulls
            )
        );
        Collections.shuffle(r, random());
        return r;
    }

    record Checks(Block.MvOrdering docValuesMvOrdering) {
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

        void bools(Block block, int position, int key) {
            BooleanVector bools = ((BooleanBlock) block).asVector();
            assertThat(bools.getBoolean(position), equalTo(key % 2 == 0));
        }

        void ids(Block block, int position, int key) {
            BytesRefVector ids = ((BytesRefBlock) block).asVector();
            assertThat(ids.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo("id"));
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
            mvLongs(block, position, key, docValuesMvOrdering);
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
            mvInts(block, position, key, docValuesMvOrdering);
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
                assertThat(ints.mvOrdering(), equalTo(docValuesMvOrdering));
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
                assertThat(ints.mvOrdering(), equalTo(docValuesMvOrdering));
            }
        }

        void mvDoubles(Block block, int position, int key) {
            DoubleBlock doubles = (DoubleBlock) block;
            int offset = doubles.getFirstValueIndex(position);
            for (int v = 0; v <= key % 3; v++) {
                assertThat(doubles.getDouble(offset + v), equalTo(key / 123_456d + v));
            }
            if (key % 3 > 0) {
                assertThat(doubles.mvOrdering(), equalTo(docValuesMvOrdering));
            }
        }

        void mvStringsFromDocValues(Block block, int position, int key) {
            mvStrings(block, position, key, docValuesMvOrdering);
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
                assertThat(bools.mvOrdering(), equalTo(docValuesMvOrdering));
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
                        "mv_text_with_delegate:row_stride:Delegating[to=mv_kwd, impl=BlockDocValuesReader.SingletonOrdinals]",
                        lessThanOrEqualTo(segmentCount)
                    )
                        .entry(
                            "mv_text_with_delegate:row_stride:Delegating[to=mv_kwd, impl=BlockDocValuesReader.Ordinals]",
                            lessThanOrEqualTo(segmentCount)
                        )
                );
            } else {
                assertMap(
                    readers,
                    matchesMap().entry(
                        "mv_text_with_delegate:column_at_a_time:Delegating[to=mv_kwd, impl=BlockDocValuesReader.SingletonOrdinals]",
                        lessThanOrEqualTo(pageCount)
                    )
                        .entry(
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
            assertMap(
                readers,
                matchesMap().entry(name + ":row_stride:BlockSourceReader." + type, count)
                    .entry("stored_fields[requires_source:true, fields:0]", count)
            );
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
            assertMap(
                readers,
                matchesMap().entry(name + ":row_stride:BlockStoredFieldsReader." + type, count)
                    .entry("stored_fields[requires_source:false, fields:1]", count)
            );
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
        MappedFieldType intFt = docValuesNumberField("i", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType longFt = docValuesNumberField("j", NumberFieldMapper.NumberType.LONG);
        MappedFieldType doubleFt = docValuesNumberField("d", NumberFieldMapper.NumberType.DOUBLE);
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
            List.of(mockSearchContext(reader)),
            ctx -> new MatchAllDocsQuery(),
            randomFrom(DataPartitioning.values()),
            randomIntBetween(1, 10),
            randomPageSize(),
            LuceneOperator.NO_LIMIT
        );
        try (
            Driver driver = new Driver(
                driverContext,
                luceneFactory.get(driverContext),
                List.of(
                    factory(reader, intFt).get(driverContext),
                    factory(reader, longFt).get(driverContext),
                    factory(reader, doubleFt).get(driverContext),
                    factory(reader, kwFt).get(driverContext)
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
                }),
                () -> {}
            )
        ) {
            runDriver(driver);
        }
        assertDriverContext(driverContext);
    }

    private NumberFieldMapper.NumberFieldType docValuesNumberField(String name, NumberFieldMapper.NumberType type) {
        return new NumberFieldMapper.NumberFieldType(name, type);
    }

    private NumberFieldMapper.NumberFieldType sourceNumberField(String name, NumberFieldMapper.NumberType type) {
        return new NumberFieldMapper.NumberFieldType(
            name,
            type,
            randomBoolean(),
            false,
            false,
            randomBoolean(),
            null,
            Map.of(),
            null,
            false,
            null,
            randomFrom(IndexMode.values())
        );
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

    private KeywordFieldMapper.KeywordFieldType sourceKeywordField(String name) {
        FieldType ft = new FieldType(KeywordFieldMapper.Defaults.FIELD_TYPE);
        ft.setDocValuesType(DocValuesType.NONE);
        ft.setStored(false);
        ft.freeze();
        return new KeywordFieldMapper.KeywordFieldType(
            name,
            ft,
            Lucene.KEYWORD_ANALYZER,
            Lucene.KEYWORD_ANALYZER,
            Lucene.KEYWORD_ANALYZER,
            new KeywordFieldMapper.Builder(name, IndexVersion.current()).docValues(false),
            false
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
}
