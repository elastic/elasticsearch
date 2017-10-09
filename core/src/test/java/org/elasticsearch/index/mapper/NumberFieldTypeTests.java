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

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;

public class NumberFieldTypeTests extends FieldTypeTestCase {

    NumberType type;

    @Before
    public void pickType() {
        type = RandomPicks.randomFrom(random(), NumberFieldMapper.NumberType.values());
    }

    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new NumberFieldMapper.NumberFieldType(type);
    }

    public void testIsFieldWithinQuery() throws IOException {
        MappedFieldType ft = createDefaultFieldType();
        // current impl ignores args and should always return INTERSECTS
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(null, randomDouble(), randomDouble(),
                randomBoolean(), randomBoolean(), null, null, null));
    }

    public void testIntegerTermsQueryWithDecimalPart() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberType.INTEGER);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(IntPoint.newSetQuery("field", 1), ft.termsQuery(Arrays.asList(1, 2.1), null));
        assertEquals(IntPoint.newSetQuery("field", 1), ft.termsQuery(Arrays.asList(1.0, 2.1), null));
        assertTrue(ft.termsQuery(Arrays.asList(1.1, 2.1), null) instanceof MatchNoDocsQuery);
    }

    public void testLongTermsQueryWithDecimalPart() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberType.LONG);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(LongPoint.newSetQuery("field", 1), ft.termsQuery(Arrays.asList(1, 2.1), null));
        assertEquals(LongPoint.newSetQuery("field", 1), ft.termsQuery(Arrays.asList(1.0, 2.1), null));
        assertTrue(ft.termsQuery(Arrays.asList(1.1, 2.1), null) instanceof MatchNoDocsQuery);
    }

    public void testByteTermQueryWithDecimalPart() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberType.BYTE);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertTrue(ft.termQuery(42.1, null) instanceof MatchNoDocsQuery);
    }

    public void testShortTermQueryWithDecimalPart() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberType.SHORT);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertTrue(ft.termQuery(42.1, null) instanceof MatchNoDocsQuery);
    }

    public void testIntegerTermQueryWithDecimalPart() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberType.INTEGER);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertTrue(ft.termQuery(42.1, null) instanceof MatchNoDocsQuery);
    }

    public void testLongTermQueryWithDecimalPart() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertTrue(ft.termQuery(42.1, null) instanceof MatchNoDocsQuery);
    }

    public void testTermQuery() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(LongPoint.newExactQuery("field", 42), ft.termQuery("42", null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.termQuery("42", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testRangeQueryWithNegativeBounds() {
        MappedFieldType ftInt = new NumberFieldMapper.NumberFieldType(NumberType.INTEGER);
        ftInt.setName("field");
        ftInt.setIndexOptions(IndexOptions.DOCS);
        assertEquals(ftInt.rangeQuery(-3, -3, true, true, null, null, null, null),
                ftInt.rangeQuery(-3.5, -2.5, true, true, null, null, null, null));
        assertEquals(ftInt.rangeQuery(-3, -3, true, true, null, null, null, null),
                ftInt.rangeQuery(-3.5, -2.5, false, false, null, null, null, null));
        assertEquals(ftInt.rangeQuery(0, 0, true, true, null, null, null, null),
                ftInt.rangeQuery(-0.5, 0.5, true, true, null, null, null, null));
        assertEquals(ftInt.rangeQuery(0, 0, true, true, null, null, null, null),
                ftInt.rangeQuery(-0.5, 0.5, false, false, null, null, null, null));
        assertEquals(ftInt.rangeQuery(1, 2, true, true, null, null, null, null),
                ftInt.rangeQuery(0.5, 2.5, true, true, null, null, null, null));
        assertEquals(ftInt.rangeQuery(1, 2, true, true, null, null, null, null),
                ftInt.rangeQuery(0.5, 2.5, false, false, null, null, null, null));
        assertEquals(ftInt.rangeQuery(0, 2, true, true, null, null, null, null),
                ftInt.rangeQuery(-0.5, 2.5, true, true, null, null, null, null));
        assertEquals(ftInt.rangeQuery(0, 2, true, true, null, null, null, null),
                ftInt.rangeQuery(-0.5, 2.5, false, false, null, null, null, null));

        assertEquals(ftInt.rangeQuery(-2, 0, true, true, null, null, null, null),
                ftInt.rangeQuery(-2.5, 0.5, true, true, null, null, null, null));
        assertEquals(ftInt.rangeQuery(-2, 0, true, true, null, null, null, null),
                ftInt.rangeQuery(-2.5, 0.5, false, false, null, null, null, null));
        assertEquals(ftInt.rangeQuery(-2, -1, true, true, null, null, null, null),
                ftInt.rangeQuery(-2.5, -0.5, true, true, null, null, null, null));
        assertEquals(ftInt.rangeQuery(-2, -1, true, true, null, null, null, null),
                ftInt.rangeQuery(-2.5, -0.5, false, false, null, null, null, null));

        MappedFieldType ftLong = new NumberFieldMapper.NumberFieldType(NumberType.LONG);
        ftLong.setName("field");
        ftLong.setIndexOptions(IndexOptions.DOCS);
        assertEquals(ftLong.rangeQuery(-3, -3, true, true, null, null, null, null),
                ftLong.rangeQuery(-3.5, -2.5, true, true, null, null, null, null));
        assertEquals(ftLong.rangeQuery(-3, -3, true, true, null, null, null, null),
                ftLong.rangeQuery(-3.5, -2.5, false, false, null, null, null, null));
        assertEquals(ftLong.rangeQuery(0, 0, true, true, null, null, null, null),
                ftLong.rangeQuery(-0.5, 0.5, true, true, null, null, null, null));
        assertEquals(ftLong.rangeQuery(0, 0, true, true, null, null, null, null),
                ftLong.rangeQuery(-0.5, 0.5, false, false, null, null, null, null));
        assertEquals(ftLong.rangeQuery(1, 2, true, true, null, null, null, null),
                ftLong.rangeQuery(0.5, 2.5, true, true, null, null, null, null));
        assertEquals(ftLong.rangeQuery(1, 2, true, true, null, null, null, null),
                ftLong.rangeQuery(0.5, 2.5, false, false, null, null, null, null));
        assertEquals(ftLong.rangeQuery(0, 2, true, true, null, null, null, null),
                ftLong.rangeQuery(-0.5, 2.5, true, true, null, null, null, null));
        assertEquals(ftLong.rangeQuery(0, 2, true, true, null, null, null, null),
                ftLong.rangeQuery(-0.5, 2.5, false, false, null, null, null, null));

        assertEquals(ftLong.rangeQuery(-2, 0, true, true, null, null, null, null),
                ftLong.rangeQuery(-2.5, 0.5, true, true, null, null, null, null));
        assertEquals(ftLong.rangeQuery(-2, 0, true, true, null, null, null, null),
                ftLong.rangeQuery(-2.5, 0.5, false, false, null, null, null, null));
        assertEquals(ftLong.rangeQuery(-2, -1, true, true, null, null, null, null),
                ftLong.rangeQuery(-2.5, -0.5, true, true, null, null, null, null));
        assertEquals(ftLong.rangeQuery(-2, -1, true, true, null, null, null, null),
                ftLong.rangeQuery(-2.5, -0.5, false, false, null, null, null, null));
    }

    public void testByteRangeQueryWithDecimalParts() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberType.BYTE);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(ft.rangeQuery(2, 10, true, true, null, null, null, null),
                ft.rangeQuery(1.1, 10, true, true, null, null, null, null));
        assertEquals(ft.rangeQuery(2, 10, true, true, null, null, null, null),
                ft.rangeQuery(1.1, 10, false, true, null, null, null, null));
        assertEquals(ft.rangeQuery(1, 10, true, true, null, null, null, null),
                ft.rangeQuery(1, 10.1, true, true, null, null, null, null));
        assertEquals(ft.rangeQuery(1, 10, true, true, null, null, null, null),
                ft.rangeQuery(1, 10.1, true, false, null, null, null, null));
    }

    public void testShortRangeQueryWithDecimalParts() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberType.SHORT);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(ft.rangeQuery(2, 10, true, true, null, null, null, null),
                ft.rangeQuery(1.1, 10, true, true, null, null, null, null));
        assertEquals(ft.rangeQuery(2, 10, true, true, null, null, null, null),
                ft.rangeQuery(1.1, 10, false, true, null, null, null, null));
        assertEquals(ft.rangeQuery(1, 10, true, true, null, null, null, null),
                ft.rangeQuery(1, 10.1, true, true, null, null, null, null));
        assertEquals(ft.rangeQuery(1, 10, true, true, null, null, null, null),
                ft.rangeQuery(1, 10.1, true, false, null, null, null, null));
    }

    public void testIntegerRangeQueryWithDecimalParts() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberType.INTEGER);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(ft.rangeQuery(2, 10, true, true, null, null, null, null),
                ft.rangeQuery(1.1, 10, true, true, null, null, null, null));
        assertEquals(ft.rangeQuery(2, 10, true, true, null, null, null, null),
                ft.rangeQuery(1.1, 10, false, true, null, null, null, null));
        assertEquals(ft.rangeQuery(1, 10, true, true, null, null, null, null),
                ft.rangeQuery(1, 10.1, true, true, null, null, null, null));
        assertEquals(ft.rangeQuery(1, 10, true, true, null, null, null, null),
                ft.rangeQuery(1, 10.1, true, false, null, null, null, null));
    }

    public void testLongRangeQueryWithDecimalParts() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberType.LONG);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(ft.rangeQuery(2, 10, true, true, null, null, null, null),
                ft.rangeQuery(1.1, 10, true, true, null, null, null, null));
        assertEquals(ft.rangeQuery(2, 10, true, true, null, null, null, null),
                ft.rangeQuery(1.1, 10, false, true, null, null, null, null));
        assertEquals(ft.rangeQuery(1, 10, true, true, null, null, null, null),
                ft.rangeQuery(1, 10.1, true, true, null, null, null, null));
        assertEquals(ft.rangeQuery(1, 10, true, true, null, null, null, null),
                ft.rangeQuery(1, 10.1, true, false, null, null, null, null));
    }

    public void testRangeQuery() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        Query expected = new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery("field", 1, 3),
                SortedNumericDocValuesField.newSlowRangeQuery("field", 1, 3));
        assertEquals(expected, ft.rangeQuery("1", "3", true, true, null, null, null, null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.rangeQuery("1", "3", true, true, null, null, null, null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testConversions() {
        assertEquals((byte) 3, NumberType.BYTE.parse(3d, true));
        assertEquals((short) 3, NumberType.SHORT.parse(3d, true));
        assertEquals(3, NumberType.INTEGER.parse(3d, true));
        assertEquals(3L, NumberType.LONG.parse(3d, true));
        assertEquals(3f, NumberType.HALF_FLOAT.parse(3d, true));
        assertEquals(3f, NumberType.FLOAT.parse(3d, true));
        assertEquals(3d, NumberType.DOUBLE.parse(3d, true));

        assertEquals((byte) 3, NumberType.BYTE.parse(3.5, true));
        assertEquals((short) 3, NumberType.SHORT.parse(3.5, true));
        assertEquals(3, NumberType.INTEGER.parse(3.5, true));
        assertEquals(3L, NumberType.LONG.parse(3.5, true));

        assertEquals(3.5f, NumberType.FLOAT.parse(3.5, true));
        assertEquals(3.5d, NumberType.DOUBLE.parse(3.5, true));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> NumberType.BYTE.parse(128, true));
        assertEquals("Value [128] is out of range for a byte", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> NumberType.SHORT.parse(65536, true));
        assertEquals("Value [65536] is out of range for a short", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> NumberType.INTEGER.parse(2147483648L, true));
        assertEquals("Value [2147483648] is out of range for an integer", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> NumberType.LONG.parse(10000000000000000000d, true));
        assertEquals("Value [1.0E19] is out of range for a long", e.getMessage());
        assertEquals(1.1f, NumberType.HALF_FLOAT.parse(1.1, true));
        assertEquals(1.1f, NumberType.FLOAT.parse(1.1, true));
        assertEquals(1.1d, NumberType.DOUBLE.parse(1.1, true));
    }

    public void testCoercions() {
        assertEquals((byte) 5, NumberType.BYTE.parse((short) 5, true));
        assertEquals((byte) 5, NumberType.BYTE.parse("5", true));
        assertEquals((byte) 5, NumberType.BYTE.parse("5.0", true));
        assertEquals((byte) 5, NumberType.BYTE.parse("5.9", true));
        assertEquals((byte) 5, NumberType.BYTE.parse(new BytesRef("5.3".getBytes(StandardCharsets.UTF_8)), true));

        assertEquals((short) 5, NumberType.SHORT.parse((byte) 5, true));
        assertEquals((short) 5, NumberType.SHORT.parse("5", true));
        assertEquals((short) 5, NumberType.SHORT.parse("5.0", true));
        assertEquals((short) 5, NumberType.SHORT.parse("5.9", true));
        assertEquals((short) 5, NumberType.SHORT.parse(new BytesRef("5.3".getBytes(StandardCharsets.UTF_8)), true));

        assertEquals(5, NumberType.INTEGER.parse((byte) 5, true));
        assertEquals(5, NumberType.INTEGER.parse("5", true));
        assertEquals(5, NumberType.INTEGER.parse("5.0", true));
        assertEquals(5, NumberType.INTEGER.parse("5.9", true));
        assertEquals(5, NumberType.INTEGER.parse(new BytesRef("5.3".getBytes(StandardCharsets.UTF_8)), true));
        assertEquals(Integer.MAX_VALUE, NumberType.INTEGER.parse(Integer.MAX_VALUE, true));

        assertEquals((long) 5, NumberType.LONG.parse((byte) 5, true));
        assertEquals((long) 5, NumberType.LONG.parse("5", true));
        assertEquals((long) 5, NumberType.LONG.parse("5.0", true));
        assertEquals((long) 5, NumberType.LONG.parse("5.9", true));
        assertEquals((long) 5, NumberType.LONG.parse(new BytesRef("5.3".getBytes(StandardCharsets.UTF_8)), true));

        // these will lose precision if they get treated as a double
        assertEquals(-4115420654264075766L, NumberType.LONG.parse("-4115420654264075766", true));
        assertEquals(-4115420654264075766L, NumberType.LONG.parse(-4115420654264075766L, true));
    }

    public void testHalfFloatRange() throws IOException {
        // make sure the accuracy loss of half floats only occurs at index time
        // this test checks that searching half floats yields the same results as
        // searching floats that are rounded to the closest half float
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        final int numDocs = 10000;
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            float value = (randomFloat() * 2 - 1) * 70000;
            float rounded = HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(value));
            doc.add(new HalfFloatPoint("half_float", value));
            doc.add(new FloatPoint("float", rounded));
            w.addDocument(doc);
        }
        final DirectoryReader reader = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        final int numQueries = 1000;
        for (int i = 0; i < numQueries; ++i) {
            float l = (randomFloat() * 2 - 1) * 65504;
            float u = (randomFloat() * 2 - 1) * 65504;
            boolean includeLower = randomBoolean();
            boolean includeUpper = randomBoolean();
            Query floatQ = NumberFieldMapper.NumberType.FLOAT.rangeQuery("float", l, u, includeLower, includeUpper, false);
            Query halfFloatQ = NumberFieldMapper.NumberType.HALF_FLOAT.rangeQuery("half_float", l, u, includeLower, includeUpper, false);
            assertEquals(searcher.count(floatQ), searcher.count(halfFloatQ));
        }
        IOUtils.close(reader, dir);
    }

    public void testNegativeZero() {
        assertEquals(
                NumberType.DOUBLE.rangeQuery("field", null, -0d, true, true, false),
                NumberType.DOUBLE.rangeQuery("field", null, +0d, true, false, false));
        assertEquals(
                NumberType.FLOAT.rangeQuery("field", null, -0f, true, true, false),
                NumberType.FLOAT.rangeQuery("field", null, +0f, true, false, false));
        assertEquals(
                NumberType.HALF_FLOAT.rangeQuery("field", null, -0f, true, true, false),
                NumberType.HALF_FLOAT.rangeQuery("field", null, +0f, true, false, false));

        assertFalse(NumberType.DOUBLE.termQuery("field", -0d).equals(NumberType.DOUBLE.termQuery("field", +0d)));
        assertFalse(NumberType.FLOAT.termQuery("field", -0f).equals(NumberType.FLOAT.termQuery("field", +0f)));
        assertFalse(NumberType.HALF_FLOAT.termQuery("field", -0f).equals(NumberType.HALF_FLOAT.termQuery("field", +0f)));
    }

    // Make sure we construct the IndexOrDocValuesQuery objects with queries that match
    // the same ranges
    public void testDocValueByteRange() throws Exception {
        doTestDocValueRangeQueries(NumberType.BYTE, () -> (byte) random().nextInt(256));
    }

    public void testDocValueShortRange() throws Exception {
        doTestDocValueRangeQueries(NumberType.SHORT, () -> (short) random().nextInt(65536));
    }

    public void testDocValueIntRange() throws Exception {
        doTestDocValueRangeQueries(NumberType.INTEGER, random()::nextInt);
    }

    public void testDocValueLongRange() throws Exception {
        doTestDocValueRangeQueries(NumberType.LONG, random()::nextLong);
    }

    public void testDocValueHalfFloatRange() throws Exception {
        doTestDocValueRangeQueries(NumberType.HALF_FLOAT, random()::nextFloat);
    }

    public void testDocValueFloatRange() throws Exception {
        doTestDocValueRangeQueries(NumberType.FLOAT, random()::nextFloat);
    }

    public void testDocValueDoubleRange() throws Exception {
        doTestDocValueRangeQueries(NumberType.DOUBLE, random()::nextDouble);
    }

    public void doTestDocValueRangeQueries(NumberType type, Supplier<Number> valueSupplier) throws Exception {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        final int numDocs = TestUtil.nextInt(random(), 100, 500);
        for (int i = 0; i < numDocs; ++i) {
            w.addDocument(type.createFields("foo", valueSupplier.get(), true, true, false));
        }
        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);
        w.close();
        final int iters = 10;
        for (int iter = 0; iter < iters; ++iter) {
            Query query = type.rangeQuery("foo",
                    random().nextBoolean() ? null : valueSupplier.get(),
                    random().nextBoolean() ? null : valueSupplier.get(),
                    randomBoolean(), randomBoolean(), true);
            assertThat(query, Matchers.instanceOf(IndexOrDocValuesQuery.class));
            IndexOrDocValuesQuery indexOrDvQuery = (IndexOrDocValuesQuery) query;
            assertEquals(
                    searcher.count(indexOrDvQuery.getIndexQuery()),
                    searcher.count(indexOrDvQuery.getRandomAccessQuery()));
        }
        reader.close();
        dir.close();
    }

    public void testParseOutOfRangeValues() throws IOException {
        final List<OutOfRangeSpec<Object>> inputs = Arrays.asList(
            OutOfRangeSpec.of(NumberType.BYTE, "128", "out of range for a byte"),
            OutOfRangeSpec.of(NumberType.BYTE, 128, "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.BYTE, -129, "is out of range for a byte"),

            OutOfRangeSpec.of(NumberType.SHORT, "32768", "out of range for a short"),
            OutOfRangeSpec.of(NumberType.SHORT, 32768, "is out of range for a short"),
            OutOfRangeSpec.of(NumberType.SHORT, -32769, "is out of range for a short"),

            OutOfRangeSpec.of(NumberType.INTEGER, "2147483648", "out of range for an integer"),
            OutOfRangeSpec.of(NumberType.INTEGER, 2147483648L, "is out of range for an integer"),
            OutOfRangeSpec.of(NumberType.INTEGER, -2147483649L, "is out of range for an integer"),

            OutOfRangeSpec.of(NumberType.LONG, "9223372036854775808", "out of range for a long"),
            OutOfRangeSpec.of(NumberType.LONG, new BigInteger("9223372036854775808"), " is out of range for a long"),
            OutOfRangeSpec.of(NumberType.LONG, new BigInteger("-9223372036854775809"), " is out of range for a long"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, "65520", "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, "3.4028235E39", "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, "1.7976931348623157E309", "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, 65520f, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, 3.4028235E39d, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, new BigDecimal("1.7976931348623157E309"), "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, -65520f, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, -3.4028235E39d, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, new BigDecimal("-1.7976931348623157E309"), "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, Float.NaN, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, Float.NaN, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, Double.NaN, "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, Float.POSITIVE_INFINITY, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, Float.POSITIVE_INFINITY, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, Double.POSITIVE_INFINITY, "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, Float.NEGATIVE_INFINITY, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, Float.NEGATIVE_INFINITY, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, Double.NEGATIVE_INFINITY, "[double] supports only finite values")
        );

        for (OutOfRangeSpec<Object> item: inputs) {
            try {
                item.type.parse(item.value, false);
                fail("Parsing exception expected for [" + item.type + "] with value [" + item.value + "]");
            } catch (IllegalArgumentException e) {
                assertThat("Incorrect error message for [" + item.type + "] with value [" + item.value + "]",
                    e.getMessage(), containsString(item.message));
            }
        }
    }

    static class OutOfRangeSpec<V> {

        final NumberType type;
        final V value;
        final String message;

        static <V> OutOfRangeSpec<V> of(NumberType t, V v, String m) {
            return new OutOfRangeSpec<>(t, v, m);
        }

        OutOfRangeSpec(NumberType t, V v, String m) {
            type = t;
            value = v;
            message = m;
        }
    }

    public void testDisplayValue() {
        for (NumberFieldMapper.NumberType type : NumberFieldMapper.NumberType.values()) {
            NumberFieldMapper.NumberFieldType fieldType = new NumberFieldMapper.NumberFieldType(type);
            assertNull(fieldType.valueForDisplay(null));
        }
        assertEquals(Byte.valueOf((byte) 3),
                new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.BYTE).valueForDisplay(3));
        assertEquals(Short.valueOf((short) 3),
                new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.SHORT).valueForDisplay(3));
        assertEquals(Integer.valueOf(3),
                new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER).valueForDisplay(3));
        assertEquals(Long.valueOf(3),
                new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG).valueForDisplay(3L));
        assertEquals(Double.valueOf(1.2),
                new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.HALF_FLOAT).valueForDisplay(1.2));
        assertEquals(Double.valueOf(1.2),
                new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.FLOAT).valueForDisplay(1.2));
        assertEquals(Double.valueOf(1.2),
                new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE).valueForDisplay(1.2));
    }
}
