/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SearchExecutionContextHelper;
import org.elasticsearch.lucene.search.XIndexSortSortedNumericDocValuesRangeQuery;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class NumberFieldTypeTests extends FieldTypeTestCase {

    NumberType type;

    @Before
    public void pickType() {
        type = RandomPicks.randomFrom(random(), NumberFieldMapper.NumberType.values());
    }

    public void testEqualsWithDifferentNumberTypes() {
        NumberType type = randomFrom(NumberType.values());
        NumberFieldType fieldType = new NumberFieldType("foo", type);

        NumberType otherType = randomValueOtherThan(type, () -> randomFrom(NumberType.values()));
        NumberFieldType otherFieldType = new NumberFieldType("foo", otherType);

        assertNotEquals(fieldType, otherFieldType);
    }

    public void testIsFieldWithinQuery() throws IOException {
        MappedFieldType ft = new NumberFieldType("field", NumberType.INTEGER);
        // current impl ignores args and should always return INTERSECTS
        assertEquals(
            Relation.INTERSECTS,
            ft.isFieldWithinQuery(null, randomDouble(), randomDouble(), randomBoolean(), randomBoolean(), null, null, null)
        );
    }

    public void testIntegerTermsQueryWithDecimalPart() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.INTEGER);
        assertEquals(IntPoint.newSetQuery("field", 1), ft.termsQuery(Arrays.asList(1, 2.1), MOCK_CONTEXT));
        assertEquals(IntPoint.newSetQuery("field", 1), ft.termsQuery(Arrays.asList(1.0, 2.1), MOCK_CONTEXT));
        assertTrue(ft.termsQuery(Arrays.asList(1.1, 2.1), MOCK_CONTEXT) instanceof MatchNoDocsQuery);
    }

    public void testLongTermsQueryWithDecimalPart() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.LONG);
        assertEquals(LongPoint.newSetQuery("field", 1), ft.termsQuery(Arrays.asList(1, 2.1), MOCK_CONTEXT));
        assertEquals(LongPoint.newSetQuery("field", 1), ft.termsQuery(Arrays.asList(1.0, 2.1), MOCK_CONTEXT));
        assertTrue(ft.termsQuery(Arrays.asList(1.1, 2.1), MOCK_CONTEXT) instanceof MatchNoDocsQuery);
    }

    public void testByteTermQueryWithDecimalPart() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.BYTE, randomBoolean());
        assertTrue(ft.termQuery(42.1, MOCK_CONTEXT) instanceof MatchNoDocsQuery);
    }

    public void testShortTermQueryWithDecimalPart() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.SHORT, randomBoolean());
        assertTrue(ft.termQuery(42.1, MOCK_CONTEXT) instanceof MatchNoDocsQuery);
    }

    public void testIntegerTermQueryWithDecimalPart() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.INTEGER, randomBoolean());
        assertTrue(ft.termQuery(42.1, MOCK_CONTEXT) instanceof MatchNoDocsQuery);
    }

    public void testLongTermQueryWithDecimalPart() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG, randomBoolean());
        assertTrue(ft.termQuery(42.1, MOCK_CONTEXT) instanceof MatchNoDocsQuery);
    }

    private static MappedFieldType unsearchable() {
        return new NumberFieldType(
            "field",
            NumberType.LONG,
            false,
            false,
            false,
            true,
            null,
            Collections.emptyMap(),
            null,
            false,
            null,
            null,
            false
        );
    }

    public void testTermQuery() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);
        assertEquals(LongPoint.newExactQuery("field", 42), ft.termQuery("42", MOCK_CONTEXT));

        ft = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG, false);
        assertEquals(SortedNumericDocValuesField.newSlowExactQuery("field", 42), ft.termQuery("42", MOCK_CONTEXT));

        MappedFieldType unsearchable = unsearchable();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("42", MOCK_CONTEXT));
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());

        MappedFieldType ft2 = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG, false);
        ElasticsearchException e2 = expectThrows(ElasticsearchException.class, () -> ft2.termQuery("42", MOCK_CONTEXT_DISALLOW_EXPENSIVE));
        assertEquals(
            "Cannot search on field [field] since it is not indexed and 'search.allow_expensive_queries' is set to false.",
            e2.getMessage()
        );
    }

    private record OutOfRangeTermQueryTestCase(NumberType type, Object value) {}

    public void testTermQueryWithOutOfRangeValues() {
        List<OutOfRangeTermQueryTestCase> testCases = List.of(
            new OutOfRangeTermQueryTestCase(NumberType.BYTE, "128"),
            new OutOfRangeTermQueryTestCase(NumberType.BYTE, 128),
            new OutOfRangeTermQueryTestCase(NumberType.BYTE, -129),
            new OutOfRangeTermQueryTestCase(NumberType.SHORT, "32768"),
            new OutOfRangeTermQueryTestCase(NumberType.SHORT, 32768),
            new OutOfRangeTermQueryTestCase(NumberType.SHORT, -32769),
            new OutOfRangeTermQueryTestCase(NumberType.INTEGER, "2147483648"),
            new OutOfRangeTermQueryTestCase(NumberType.INTEGER, 2147483648L),
            new OutOfRangeTermQueryTestCase(NumberType.INTEGER, -2147483649L),
            new OutOfRangeTermQueryTestCase(NumberType.LONG, "9223372036854775808"),
            new OutOfRangeTermQueryTestCase(NumberType.LONG, new BigInteger("9223372036854775808")),
            new OutOfRangeTermQueryTestCase(NumberType.LONG, new BigInteger("-9223372036854775809")),
            new OutOfRangeTermQueryTestCase(NumberType.HALF_FLOAT, "65520"),
            new OutOfRangeTermQueryTestCase(NumberType.HALF_FLOAT, 65520f),
            new OutOfRangeTermQueryTestCase(NumberType.HALF_FLOAT, -65520f),
            new OutOfRangeTermQueryTestCase(NumberType.HALF_FLOAT, Float.POSITIVE_INFINITY),
            new OutOfRangeTermQueryTestCase(NumberType.HALF_FLOAT, Float.NEGATIVE_INFINITY),
            new OutOfRangeTermQueryTestCase(NumberType.HALF_FLOAT, Float.NaN),
            new OutOfRangeTermQueryTestCase(NumberType.FLOAT, "3.4028235E39"),
            new OutOfRangeTermQueryTestCase(NumberType.FLOAT, 3.4028235E39d),
            new OutOfRangeTermQueryTestCase(NumberType.FLOAT, -3.4028235E39d),
            new OutOfRangeTermQueryTestCase(NumberType.FLOAT, Float.POSITIVE_INFINITY),
            new OutOfRangeTermQueryTestCase(NumberType.FLOAT, Float.NEGATIVE_INFINITY),
            new OutOfRangeTermQueryTestCase(NumberType.FLOAT, Float.NaN),
            new OutOfRangeTermQueryTestCase(NumberType.DOUBLE, "1.7976931348623157E309"),
            new OutOfRangeTermQueryTestCase(NumberType.DOUBLE, new BigDecimal("1.7976931348623157E309")),
            new OutOfRangeTermQueryTestCase(NumberType.DOUBLE, new BigDecimal("-1.7976931348623157E309")),
            new OutOfRangeTermQueryTestCase(NumberType.DOUBLE, Double.NaN),
            new OutOfRangeTermQueryTestCase(NumberType.DOUBLE, Double.POSITIVE_INFINITY),
            new OutOfRangeTermQueryTestCase(NumberType.DOUBLE, Double.NEGATIVE_INFINITY)
        );

        for (OutOfRangeTermQueryTestCase testCase : testCases) {
            assertTrue(testCase.type.termQuery("field", testCase.value, randomBoolean()) instanceof MatchNoDocsQuery);
        }
    }

    public void testRangeQueryWithNegativeBounds() {
        MappedFieldType ftInt = new NumberFieldMapper.NumberFieldType("field", NumberType.INTEGER, randomBoolean());
        assertEquals(
            ftInt.rangeQuery(-3, -3, true, true, null, null, null, MOCK_CONTEXT),
            ftInt.rangeQuery(-3.5, -2.5, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftInt.rangeQuery(-3, -3, true, true, null, null, null, MOCK_CONTEXT),
            ftInt.rangeQuery(-3.5, -2.5, false, false, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftInt.rangeQuery(0, 0, true, true, null, null, null, MOCK_CONTEXT),
            ftInt.rangeQuery(-0.5, 0.5, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftInt.rangeQuery(0, 0, true, true, null, null, null, MOCK_CONTEXT),
            ftInt.rangeQuery(-0.5, 0.5, false, false, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftInt.rangeQuery(1, 2, true, true, null, null, null, MOCK_CONTEXT),
            ftInt.rangeQuery(0.5, 2.5, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftInt.rangeQuery(1, 2, true, true, null, null, null, MOCK_CONTEXT),
            ftInt.rangeQuery(0.5, 2.5, false, false, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftInt.rangeQuery(0, 2, true, true, null, null, null, MOCK_CONTEXT),
            ftInt.rangeQuery(-0.5, 2.5, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftInt.rangeQuery(0, 2, true, true, null, null, null, MOCK_CONTEXT),
            ftInt.rangeQuery(-0.5, 2.5, false, false, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ftInt.rangeQuery(-2, 0, true, true, null, null, null, MOCK_CONTEXT),
            ftInt.rangeQuery(-2.5, 0.5, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftInt.rangeQuery(-2, 0, true, true, null, null, null, MOCK_CONTEXT),
            ftInt.rangeQuery(-2.5, 0.5, false, false, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftInt.rangeQuery(-2, -1, true, true, null, null, null, MOCK_CONTEXT),
            ftInt.rangeQuery(-2.5, -0.5, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftInt.rangeQuery(-2, -1, true, true, null, null, null, MOCK_CONTEXT),
            ftInt.rangeQuery(-2.5, -0.5, false, false, null, null, null, MOCK_CONTEXT)
        );

        MappedFieldType ftLong = new NumberFieldMapper.NumberFieldType("field", NumberType.LONG, randomBoolean());
        assertEquals(
            ftLong.rangeQuery(-3, -3, true, true, null, null, null, MOCK_CONTEXT),
            ftLong.rangeQuery(-3.5, -2.5, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftLong.rangeQuery(-3, -3, true, true, null, null, null, MOCK_CONTEXT),
            ftLong.rangeQuery(-3.5, -2.5, false, false, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftLong.rangeQuery(0, 0, true, true, null, null, null, MOCK_CONTEXT),
            ftLong.rangeQuery(-0.5, 0.5, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftLong.rangeQuery(0, 0, true, true, null, null, null, MOCK_CONTEXT),
            ftLong.rangeQuery(-0.5, 0.5, false, false, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftLong.rangeQuery(1, 2, true, true, null, null, null, MOCK_CONTEXT),
            ftLong.rangeQuery(0.5, 2.5, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftLong.rangeQuery(1, 2, true, true, null, null, null, MOCK_CONTEXT),
            ftLong.rangeQuery(0.5, 2.5, false, false, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftLong.rangeQuery(0, 2, true, true, null, null, null, MOCK_CONTEXT),
            ftLong.rangeQuery(-0.5, 2.5, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftLong.rangeQuery(0, 2, true, true, null, null, null, MOCK_CONTEXT),
            ftLong.rangeQuery(-0.5, 2.5, false, false, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ftLong.rangeQuery(-2, 0, true, true, null, null, null, MOCK_CONTEXT),
            ftLong.rangeQuery(-2.5, 0.5, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftLong.rangeQuery(-2, 0, true, true, null, null, null, MOCK_CONTEXT),
            ftLong.rangeQuery(-2.5, 0.5, false, false, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftLong.rangeQuery(-2, -1, true, true, null, null, null, MOCK_CONTEXT),
            ftLong.rangeQuery(-2.5, -0.5, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ftLong.rangeQuery(-2, -1, true, true, null, null, null, MOCK_CONTEXT),
            ftLong.rangeQuery(-2.5, -0.5, false, false, null, null, null, MOCK_CONTEXT)
        );
    }

    public void testByteRangeQueryWithDecimalParts() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.BYTE, randomBoolean());
        assertEquals(
            ft.rangeQuery(2, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1.1, 10, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ft.rangeQuery(2, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1.1, 10, false, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ft.rangeQuery(1, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1, 10.1, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ft.rangeQuery(1, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1, 10.1, true, false, null, null, null, MOCK_CONTEXT)
        );
    }

    public void testShortRangeQueryWithDecimalParts() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.SHORT, randomBoolean());
        assertEquals(
            ft.rangeQuery(2, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1.1, 10, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ft.rangeQuery(2, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1.1, 10, false, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ft.rangeQuery(1, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1, 10.1, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ft.rangeQuery(1, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1, 10.1, true, false, null, null, null, MOCK_CONTEXT)
        );
    }

    public void testIntegerRangeQueryWithDecimalParts() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.INTEGER, randomBoolean());
        assertEquals(
            ft.rangeQuery(2, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1.1, 10, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ft.rangeQuery(2, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1.1, 10, false, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ft.rangeQuery(1, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1, 10.1, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ft.rangeQuery(1, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1, 10.1, true, false, null, null, null, MOCK_CONTEXT)
        );
    }

    public void testLongRangeQueryWithDecimalParts() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.LONG, randomBoolean());
        assertEquals(
            ft.rangeQuery(2, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1.1, 10, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ft.rangeQuery(2, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1.1, 10, false, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ft.rangeQuery(1, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1, 10.1, true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals(
            ft.rangeQuery(1, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(1, 10.1, true, false, null, null, null, MOCK_CONTEXT)
        );
    }

    public void testHalfFloatRangeQueryWithOverflowingBounds() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.HALF_FLOAT, randomBoolean());
        final float min_half_float = -65504;
        final float max_half_float = 65504;
        assertEquals(
            ft.rangeQuery(min_half_float, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(-1e+300, 10, true, true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(min_half_float, 10, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(Float.NEGATIVE_INFINITY, 10, true, true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(10, max_half_float, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(10, 1e+300, true, true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(10, max_half_float, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(10, Float.POSITIVE_INFINITY, true, true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(10, 1e+300, false, false, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(10, Float.POSITIVE_INFINITY, false, false, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(-1e+300, 10, false, false, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(Float.NEGATIVE_INFINITY, 10, false, false, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(10, 1e+300, false, false, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(10, max_half_float, false, true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(-1e+300, 10, false, false, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(min_half_float, 10, true, false, null, null, null, MOCK_CONTEXT)
        );
    }

    public void testFloatRangeQueryWithOverflowingBounds() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.FLOAT, randomBoolean());

        assertEquals(
            ft.rangeQuery(-Float.MAX_VALUE, 10.0, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(-1e+300, 10.0, true, true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(-Float.MAX_VALUE, 10.0, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(Float.NEGATIVE_INFINITY, 10.0, true, true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(10, Float.MAX_VALUE, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(10, 1e+300, true, true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(10, Float.MAX_VALUE, true, true, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(10, Float.POSITIVE_INFINITY, true, true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(10, 1e+300, false, false, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(10, Float.POSITIVE_INFINITY, false, false, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(-1e+300, 10, false, false, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(Float.NEGATIVE_INFINITY, 10, false, false, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(10, 1e+300, false, false, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(10, Float.MAX_VALUE, false, true, null, null, null, MOCK_CONTEXT)
        );

        assertEquals(
            ft.rangeQuery(-1e+300, 10, false, false, null, null, null, MOCK_CONTEXT),
            ft.rangeQuery(-Float.MAX_VALUE, 10, true, false, null, null, null, MOCK_CONTEXT)
        );
    }

    public void testRangeQuery() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);
        Query expected = new IndexOrDocValuesQuery(
            LongPoint.newRangeQuery("field", 1, 3),
            SortedNumericDocValuesField.newSlowRangeQuery("field", 1, 3)
        );
        assertEquals(expected, ft.rangeQuery("1", "3", true, true, null, null, null, MOCK_CONTEXT));

        ft = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG, false);
        expected = SortedNumericDocValuesField.newSlowRangeQuery("field", 1, 3);
        assertEquals(expected, ft.rangeQuery("1", "3", true, true, null, null, null, MOCK_CONTEXT));

        MappedFieldType unsearchable = unsearchable();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.rangeQuery("1", "3", true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());

        MappedFieldType ft2 = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG, false);
        ElasticsearchException e2 = expectThrows(
            ElasticsearchException.class,
            () -> ft2.rangeQuery("1", "3", true, true, null, null, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "Cannot search on field [field] since it is not indexed and 'search.allow_expensive_queries' is set to false.",
            e2.getMessage()
        );
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
        assertEquals(1.0996094f, NumberType.HALF_FLOAT.parse(1.1, true));  // Half float loses a bit of precision even on 1.1....
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
            // Note: this test purposefully allows half-floats to be indexed over their dynamic range (65504), which
            // ends up being rounded to Infinity by halfFloatToSortableShort()
            float value = (randomFloat() * 2 - 1) * 70000;
            float rounded = HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(value));
            doc.add(new HalfFloatPoint("half_float", value));
            doc.add(new SortedNumericDocValuesField("half_float", HalfFloatPoint.halfFloatToSortableShort(value)));
            doc.add(new FloatPoint("float", rounded));
            doc.add(new SortedNumericDocValuesField("float", NumericUtils.floatToSortableInt(rounded)));
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
            Query floatQ = NumberType.FLOAT.rangeQuery(
                "float",
                l,
                u,
                includeLower,
                includeUpper,
                randomBoolean(),
                MOCK_CONTEXT,
                randomBoolean()
            );
            Query halfFloatQ = NumberType.HALF_FLOAT.rangeQuery(
                "half_float",
                l,
                u,
                includeLower,
                includeUpper,
                randomBoolean(),
                MOCK_CONTEXT,
                randomBoolean()
            );
            assertEquals(searcher.count(floatQ), searcher.count(halfFloatQ));
        }
        IOUtils.close(reader, dir);
    }

    public void testNegativeZero() {
        final boolean isIndexed = randomBoolean();
        assertEquals(
            NumberType.DOUBLE.rangeQuery("field", null, -0d, true, true, false, MOCK_CONTEXT, isIndexed),
            NumberType.DOUBLE.rangeQuery("field", null, +0d, true, false, false, MOCK_CONTEXT, isIndexed)
        );
        assertEquals(
            NumberType.FLOAT.rangeQuery("field", null, -0f, true, true, false, MOCK_CONTEXT, isIndexed),
            NumberType.FLOAT.rangeQuery("field", null, +0f, true, false, false, MOCK_CONTEXT, isIndexed)
        );
        assertEquals(
            NumberType.HALF_FLOAT.rangeQuery("field", null, -0f, true, true, false, MOCK_CONTEXT, isIndexed),
            NumberType.HALF_FLOAT.rangeQuery("field", null, +0f, true, false, false, MOCK_CONTEXT, isIndexed)
        );

        assertNotEquals(NumberType.DOUBLE.termQuery("field", -0d, isIndexed), NumberType.DOUBLE.termQuery("field", +0d, isIndexed));
        assertNotEquals(NumberType.FLOAT.termQuery("field", -0f, isIndexed), NumberType.FLOAT.termQuery("field", +0f, isIndexed));
        assertNotEquals(NumberType.HALF_FLOAT.termQuery("field", -0f, isIndexed), NumberType.HALF_FLOAT.termQuery("field", +0f, isIndexed));
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
            final LuceneDocument doc = new LuceneDocument();
            type.addFields(doc, "foo", valueSupplier.get(), true, true, false);
            w.addDocument(doc);
        }
        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);
        w.close();
        final int iters = 10;
        for (int iter = 0; iter < iters; ++iter) {
            Query query = type.rangeQuery(
                "foo",
                random().nextBoolean() ? null : valueSupplier.get(),
                random().nextBoolean() ? null : valueSupplier.get(),
                randomBoolean(),
                randomBoolean(),
                true,
                MOCK_CONTEXT,
                true
            );
            assertThat(query, instanceOf(IndexOrDocValuesQuery.class));
            IndexOrDocValuesQuery indexOrDvQuery = (IndexOrDocValuesQuery) query;
            assertEquals(searcher.count(indexOrDvQuery.getIndexQuery()), searcher.count(indexOrDvQuery.getRandomAccessQuery()));
        }
        reader.close();
        dir.close();
    }

    public void testIndexSortIntRange() throws Exception {
        doTestIndexSortRangeQueries(NumberType.INTEGER, random()::nextInt);
    }

    public void testIndexSortLongRange() throws Exception {
        doTestIndexSortRangeQueries(NumberType.LONG, random()::nextLong);
    }

    public void doTestIndexSortRangeQueries(NumberType type, Supplier<Number> valueSupplier) throws IOException {
        // Create index settings with an index sort.
        Settings settings = indexSettings(IndexVersion.current(), 1, 1).put("index.sort.field", "field").build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);

        // Create an index writer configured with the same index sort.
        NumberFieldType fieldType = new NumberFieldType("field", type);
        IndexNumericFieldData fielddata = (IndexNumericFieldData) fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("test"))
            .build(null, null);
        SortField sortField = fielddata.sortField(null, MultiValueMode.MIN, null, randomBoolean());

        IndexWriterConfig writerConfig = new IndexWriterConfig();
        writerConfig.setIndexSort(new Sort(sortField));

        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, writerConfig);
        final int numDocs = TestUtil.nextInt(random(), 100, 500);
        for (int i = 0; i < numDocs; ++i) {
            final LuceneDocument doc = new LuceneDocument();
            type.addFields(doc, "field", valueSupplier.get(), true, true, false);
            w.addDocument(doc);
        }

        // Ensure that the optimized index sort query gives the same results as a points query.
        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);

        SearchExecutionContext context = SearchExecutionContextHelper.createSimple(indexSettings, parserConfig(), writableRegistry());

        final int iters = 10;
        for (int iter = 0; iter < iters; ++iter) {
            boolean isIndexed = randomBoolean();
            Query query = type.rangeQuery(
                "field",
                random().nextBoolean() ? null : valueSupplier.get(),
                random().nextBoolean() ? null : valueSupplier.get(),
                randomBoolean(),
                randomBoolean(),
                true,
                context,
                isIndexed
            );
            assertThat(query, instanceOf(XIndexSortSortedNumericDocValuesRangeQuery.class));
            Query fallbackQuery = ((XIndexSortSortedNumericDocValuesRangeQuery) query).getFallbackQuery();

            if (isIndexed) {
                assertThat(fallbackQuery, instanceOf(IndexOrDocValuesQuery.class));
                IndexOrDocValuesQuery indexOrDvQuery = (IndexOrDocValuesQuery) fallbackQuery;
                assertEquals(searcher.count(query), searcher.count(indexOrDvQuery.getIndexQuery()));
            } else {
                assertEquals(searcher.count(query), searcher.count(fallbackQuery));
            }
        }

        reader.close();
        w.close();
        dir.close();
    }

    public void testParseOutOfRangeValues() throws IOException {
        final List<OutOfRangeSpec> inputs = Arrays.asList(
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

        for (OutOfRangeSpec item : inputs) {
            try {
                item.type.parse(item.value, false);
                fail("Parsing exception expected for [" + item.type + "] with value [" + item.value + "]");
            } catch (IllegalArgumentException e) {
                assertThat(
                    "Incorrect error message for [" + item.type + "] with value [" + item.value + "]",
                    e.getMessage(),
                    containsString(item.message)
                );
            }
        }
    }

    static class OutOfRangeSpec {

        final NumberType type;
        final Object value;
        final String message;

        static OutOfRangeSpec of(NumberType t, Object v, String m) {
            return new OutOfRangeSpec(t, v, m);
        }

        OutOfRangeSpec(NumberType t, Object v, String m) {
            type = t;
            value = v;
            message = m;
        }

        public void write(XContentBuilder b) throws IOException {
            if (value instanceof BigInteger) {
                b.rawField("field", new ByteArrayInputStream(value.toString().getBytes("UTF-8")), XContentType.JSON);
            } else {
                b.field("field", value);
            }
        }
    }

    public void testDisplayValue() {
        for (NumberFieldMapper.NumberType type : NumberFieldMapper.NumberType.values()) {
            NumberFieldMapper.NumberFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", type);
            assertNull(fieldType.valueForDisplay(null));
        }
        assertEquals(
            Byte.valueOf((byte) 3),
            new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.BYTE).valueForDisplay(3)
        );
        assertEquals(
            Short.valueOf((short) 3),
            new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.SHORT).valueForDisplay(3)
        );
        assertEquals(
            Integer.valueOf(3),
            new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.INTEGER).valueForDisplay(3)
        );
        assertEquals(
            Long.valueOf(3),
            new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG).valueForDisplay(3L)
        );
        assertEquals(
            Double.valueOf(1.2),
            new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.HALF_FLOAT).valueForDisplay(1.2)
        );
        assertEquals(
            Double.valueOf(1.2),
            new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.FLOAT).valueForDisplay(1.2)
        );
        assertEquals(
            Double.valueOf(1.2),
            new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE).valueForDisplay(1.2)
        );
    }

    public void testParsePoint() {
        {
            byte[] bytes = new byte[Integer.BYTES];
            byte value = randomByte();
            IntPoint.encodeDimension(value, bytes, 0);
            assertThat(NumberType.BYTE.parsePoint(bytes), equalTo(value));
        }
        {
            byte[] bytes = new byte[Integer.BYTES];
            short value = randomShort();
            IntPoint.encodeDimension(value, bytes, 0);
            assertThat(NumberType.SHORT.parsePoint(bytes), equalTo(value));
        }
        {
            byte[] bytes = new byte[Integer.BYTES];
            int value = randomInt();
            IntPoint.encodeDimension(value, bytes, 0);
            assertThat(NumberType.INTEGER.parsePoint(bytes), equalTo(value));
        }
        {
            byte[] bytes = new byte[Long.BYTES];
            long value = randomLong();
            LongPoint.encodeDimension(value, bytes, 0);
            assertThat(NumberType.LONG.parsePoint(bytes), equalTo(value));
        }
        {
            byte[] bytes = new byte[Float.BYTES];
            float value = randomFloat();
            FloatPoint.encodeDimension(value, bytes, 0);
            assertThat(NumberType.FLOAT.parsePoint(bytes), equalTo(value));
        }
        {
            byte[] bytes = new byte[Double.BYTES];
            double value = randomDouble();
            DoublePoint.encodeDimension(value, bytes, 0);
            assertThat(NumberType.DOUBLE.parsePoint(bytes), equalTo(value));
        }
        {
            byte[] bytes = new byte[Float.BYTES];
            float value = 3f;
            HalfFloatPoint.encodeDimension(value, bytes, 0);
            assertThat(NumberType.HALF_FLOAT.parsePoint(bytes), equalTo(value));
        }
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new NumberFieldMapper.Builder(
            "field",
            NumberType.INTEGER,
            ScriptCompiler.NONE,
            false,
            true,
            IndexVersion.current(),
            null,
            null
        ).build(MapperBuilderContext.root(false, false)).fieldType();
        assertEquals(List.of(3), fetchSourceValue(mapper, 3.14));
        assertEquals(List.of(42), fetchSourceValue(mapper, "42.9"));
        assertEquals(List.of(3, 42), fetchSourceValues(mapper, 3.14, "foo", "42.9"));

        MappedFieldType nullValueMapper = new NumberFieldMapper.Builder(
            "field",
            NumberType.FLOAT,
            ScriptCompiler.NONE,
            false,
            true,
            IndexVersion.current(),
            null,
            null
        ).nullValue(2.71f).build(MapperBuilderContext.root(false, false)).fieldType();
        assertEquals(List.of(2.71f), fetchSourceValue(nullValueMapper, ""));
        assertEquals(List.of(2.71f), fetchSourceValue(nullValueMapper, null));
    }

    public void testFetchHalfFloatFromSource() throws IOException {
        MappedFieldType mapper = new NumberFieldMapper.Builder(
            "field",
            NumberType.HALF_FLOAT,
            ScriptCompiler.NONE,
            false,
            true,
            IndexVersion.current(),
            null,
            null
        ).build(MapperBuilderContext.root(false, false)).fieldType();
        /*
         * Half float loses a fair bit of precision compared to float but
         * we still do floating point comparisons. The "funny" trailing
         * {@code .000625} is, for example, the precision loss of using
         * a half float reflected back into a float.
         */
        assertEquals(List.of(3.140625F), fetchSourceValue(mapper, 3.14));
        assertEquals(List.of(3.140625F), fetchSourceValue(mapper, 3.14F));
        assertEquals(List.of(3.0F), fetchSourceValue(mapper, 3));
        assertEquals(List.of(42.90625F), fetchSourceValue(mapper, "42.9"));
        assertEquals(List.of(47.125F), fetchSourceValue(mapper, 47.1231234));
        assertEquals(List.of(3.140625F, 42.90625F), fetchSourceValues(mapper, 3.14, "foo", "42.9"));
    }
}
