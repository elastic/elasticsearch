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
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.lucene.LuceneSourceOperatorTests.mockSearchContext;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

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
        return factory(reader, new NumberFieldMapper.NumberFieldType("long", NumberFieldMapper.NumberType.LONG));
    }

    static Operator.OperatorFactory factory(IndexReader reader, MappedFieldType ft) {
        return factory(reader, ft.name(), ft.blockLoader(null));
    }

    static Operator.OperatorFactory factory(IndexReader reader, String name, BlockLoader loader) {
        return new ValuesSourceReaderOperator.Factory(
            List.of(new ValuesSourceReaderOperator.FieldInfo(name, List.of(loader))),
            List.of(reader),
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
        interface Check {
            void check(Block block, int position, int key);
        }
        record Info(ValuesSourceReaderOperator.FieldInfo info, Check check) {
            Info(MappedFieldType ft, Check check) {
                this(fieldInfo(ft), check);
            }
        }
        class Checks {
            void checkLongs(Block block, int position, int key) {
                LongVector longs = ((LongBlock) block).asVector();
                assertThat(longs.getLong(position), equalTo((long) key));
            }

            void checkInts(Block block, int position, int key) {
                IntVector ints = ((IntBlock) block).asVector();
                assertThat(ints.getInt(position), equalTo(key));
            }

            void checkShorts(Block block, int position, int key) {
                IntVector ints = ((IntBlock) block).asVector();
                assertThat(ints.getInt(position), equalTo((int) (short) key));
            }

            void checkBytes(Block block, int position, int key) {
                IntVector ints = ((IntBlock) block).asVector();
                assertThat(ints.getInt(position), equalTo((int) (byte) key));
            }

            void checkDoubles(Block block, int position, int key) {
                DoubleVector doubles = ((DoubleBlock) block).asVector();
                assertThat(doubles.getDouble(position), equalTo(key / 123_456d));
            }

            void checkStrings(Block block, int position, int key) {
                BytesRefVector keywords = ((BytesRefBlock) block).asVector();
                assertThat(keywords.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo(Integer.toString(key)));
            }

            void checkBools(Block block, int position, int key) {
                BooleanVector bools = ((BooleanBlock) block).asVector();
                assertThat(bools.getBoolean(position), equalTo(key % 2 == 0));
            }

            void checkIds(Block block, int position, int key) {
                BytesRefVector ids = ((BytesRefBlock) block).asVector();
                assertThat(ids.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo("id"));
            }

            void checkConstantBytes(Block block, int position, int key) {
                BytesRefVector keywords = ((BytesRefBlock) block).asVector();
                assertThat(keywords.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo("foo"));
            }

            void checkConstantNulls(Block block, int position, int key) {
                assertTrue(block.areAllValuesNull());
                assertTrue(block.isNull(position));
            }

            void checkMvLongsFromDocValues(Block block, int position, int key) {
                checkMvLongs(block, position, key, docValuesMvOrdering);
            }

            void checkMvLongsUnordered(Block block, int position, int key) {
                checkMvLongs(block, position, key, Block.MvOrdering.UNORDERED);
            }

            private void checkMvLongs(Block block, int position, int key, Block.MvOrdering expectedMv) {
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

            void checkMvIntsFromDocValues(Block block, int position, int key) {
                checkMvInts(block, position, key, docValuesMvOrdering);
            }

            void checkMvIntsUnordered(Block block, int position, int key) {
                checkMvInts(block, position, key, Block.MvOrdering.UNORDERED);
            }

            private void checkMvInts(Block block, int position, int key, Block.MvOrdering expectedMv) {
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

            void checkMvShorts(Block block, int position, int key) {
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

            void checkMvBytes(Block block, int position, int key) {
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

            void checkMvDoubles(Block block, int position, int key) {
                DoubleBlock doubles = (DoubleBlock) block;
                int offset = doubles.getFirstValueIndex(position);
                for (int v = 0; v <= key % 3; v++) {
                    assertThat(doubles.getDouble(offset + v), equalTo(key / 123_456d + v));
                }
                if (key % 3 > 0) {
                    assertThat(doubles.mvOrdering(), equalTo(docValuesMvOrdering));
                }
            }

            void checkMvStringsFromDocValues(Block block, int position, int key) {
                checkMvStrings(block, position, key, docValuesMvOrdering);
            }

            void checkMvStringsUnordered(Block block, int position, int key) {
                checkMvStrings(block, position, key, Block.MvOrdering.UNORDERED);
            }

            void checkMvStrings(Block block, int position, int key, Block.MvOrdering expectedMv) {
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

            void checkMvBools(Block block, int position, int key) {
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
        Checks checks = new Checks();
        List<Info> infos = new ArrayList<>();
        infos.add(new Info(new NumberFieldMapper.NumberFieldType("long", NumberFieldMapper.NumberType.LONG), checks::checkLongs));
        infos.add(
            new Info(new NumberFieldMapper.NumberFieldType("mv_long", NumberFieldMapper.NumberType.LONG), checks::checkMvLongsFromDocValues)
        );
        infos.add(new Info(sourceNumberField("source_long", NumberFieldMapper.NumberType.LONG), checks::checkLongs));
        infos.add(new Info(sourceNumberField("mv_source_long", NumberFieldMapper.NumberType.LONG), checks::checkMvLongsUnordered));
        infos.add(new Info(new NumberFieldMapper.NumberFieldType("int", NumberFieldMapper.NumberType.INTEGER), checks::checkInts));
        infos.add(
            new Info(
                new NumberFieldMapper.NumberFieldType("mv_int", NumberFieldMapper.NumberType.INTEGER),
                checks::checkMvIntsFromDocValues
            )
        );
        infos.add(new Info(sourceNumberField("source_int", NumberFieldMapper.NumberType.INTEGER), checks::checkInts));
        infos.add(new Info(sourceNumberField("mv_source_int", NumberFieldMapper.NumberType.INTEGER), checks::checkMvIntsUnordered));
        infos.add(new Info(new NumberFieldMapper.NumberFieldType("short", NumberFieldMapper.NumberType.SHORT), checks::checkShorts));
        infos.add(new Info(new NumberFieldMapper.NumberFieldType("mv_short", NumberFieldMapper.NumberType.SHORT), checks::checkMvShorts));
        infos.add(new Info(new NumberFieldMapper.NumberFieldType("byte", NumberFieldMapper.NumberType.BYTE), checks::checkBytes));
        infos.add(new Info(new NumberFieldMapper.NumberFieldType("mv_byte", NumberFieldMapper.NumberType.BYTE), checks::checkMvBytes));
        infos.add(new Info(new NumberFieldMapper.NumberFieldType("double", NumberFieldMapper.NumberType.DOUBLE), checks::checkDoubles));
        infos.add(
            new Info(new NumberFieldMapper.NumberFieldType("mv_double", NumberFieldMapper.NumberType.DOUBLE), checks::checkMvDoubles)
        );
        infos.add(new Info(new KeywordFieldMapper.KeywordFieldType("kwd"), checks::checkStrings));
        infos.add(new Info(new KeywordFieldMapper.KeywordFieldType("mv_kwd"), checks::checkMvStringsFromDocValues));
        infos.add(new Info(storedKeywordField("stored_kwd"), checks::checkStrings));
        infos.add(new Info(storedKeywordField("mv_stored_kwd"), checks::checkMvStringsUnordered));
        infos.add(new Info(new TextFieldMapper.TextFieldType("source_text", false), checks::checkStrings));
        infos.add(new Info(new TextFieldMapper.TextFieldType("mv_source_text", false), checks::checkMvStringsUnordered));
        infos.add(new Info(storedTextField("stored_text"), checks::checkStrings));
        infos.add(new Info(storedTextField("mv_stored_text"), checks::checkMvStringsUnordered));
        infos.add(
            new Info(textFieldWithDelegate("text_with_delegate", new KeywordFieldMapper.KeywordFieldType("kwd")), checks::checkStrings)
        );
        infos.add(
            new Info(
                textFieldWithDelegate("mv_text_with_delegate", new KeywordFieldMapper.KeywordFieldType("mv_kwd")),
                checks::checkMvStringsFromDocValues
            )
        );
        infos.add(new Info(new BooleanFieldMapper.BooleanFieldType("bool"), checks::checkBools));
        infos.add(new Info(new BooleanFieldMapper.BooleanFieldType("mv_bool"), checks::checkMvBools));
        infos.add(new Info(new ProvidedIdFieldMapper(() -> false).fieldType(), checks::checkIds));
        infos.add(new Info(TsidExtractingIdFieldMapper.INSTANCE.fieldType(), checks::checkIds));
        infos.add(
            new Info(
                new ValuesSourceReaderOperator.FieldInfo("constant_bytes", List.of(BlockLoader.constantBytes(new BytesRef("foo")))),
                checks::checkConstantBytes
            )
        );
        infos.add(
            new Info(new ValuesSourceReaderOperator.FieldInfo("null", List.of(BlockLoader.CONSTANT_NULLS)), checks::checkConstantNulls)
        );

        List<Operator> operators = new ArrayList<>();
        Collections.shuffle(infos, random());

        operators.add(
            new ValuesSourceReaderOperator.Factory(
                List.of(fieldInfo(new NumberFieldMapper.NumberFieldType("key", NumberFieldMapper.NumberType.INTEGER))),
                List.of(reader),
                0
            ).get(driverContext)
        );
        List<Info> tests = new ArrayList<>();
        while (infos.isEmpty() == false) {
            List<Info> b = randomNonEmptySubsetOf(infos);
            infos.removeAll(b);
            tests.addAll(b);
            operators.add(
                new ValuesSourceReaderOperator.Factory(b.stream().map(i -> i.info).toList(), List.of(reader), 0).get(driverContext)
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
                        tests.get(i).check.check(page.getBlock(2 + i), p, key);
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

    public void testWithNulls() throws IOException {
        MappedFieldType intFt = new NumberFieldMapper.NumberFieldType("i", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType longFt = new NumberFieldMapper.NumberFieldType("j", NumberFieldMapper.NumberType.LONG);
        MappedFieldType doubleFt = new NumberFieldMapper.NumberFieldType("d", NumberFieldMapper.NumberType.DOUBLE);
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
