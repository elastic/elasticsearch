/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper.UnsignedLongFieldType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper.BIGINTEGER_2_64_MINUS_ONE;
import static org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper.UnsignedLongFieldType.parseLowerRangeTerm;
import static org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper.UnsignedLongFieldType.parseTerm;
import static org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper.UnsignedLongFieldType.parseUpperRangeTerm;

public class UnsignedLongFieldTypeTests extends FieldTypeTestCase {

    public void testTermQuery() {
        UnsignedLongFieldType ft = new UnsignedLongFieldType("my_unsigned_long");

        assertEquals(LongPoint.newExactQuery("my_unsigned_long", -9223372036854775808L), ft.termQuery(0, null));
        assertEquals(LongPoint.newExactQuery("my_unsigned_long", 0L), ft.termQuery("9223372036854775808", null));
        assertEquals(LongPoint.newExactQuery("my_unsigned_long", 9223372036854775807L), ft.termQuery("18446744073709551615", null));

        assertEquals(new MatchNoDocsQuery(), ft.termQuery(-1L, null));
        assertEquals(new MatchNoDocsQuery(), ft.termQuery(10.5, null));
        assertEquals(new MatchNoDocsQuery(), ft.termQuery("18446744073709551616", null));

        expectThrows(NumberFormatException.class, () -> ft.termQuery("18incorrectnumber", null));
    }

    public void testTermsQuery() {
        UnsignedLongFieldType ft = new UnsignedLongFieldType("my_unsigned_long");

        assertEquals(
            LongPoint.newSetQuery("my_unsigned_long", -9223372036854775808L, 0L, 9223372036854775807L),
            ft.termsQuery(List.of("0", "9223372036854775808", "18446744073709551615"), null)
        );

        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(List.of(-9223372036854775808L, -1L), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(List.of("-0.5", "3.14", "18446744073709551616"), null));

        expectThrows(NumberFormatException.class, () -> ft.termsQuery(List.of("18incorrectnumber"), null));
    }

    public void testRangeQuery() {
        UnsignedLongFieldType ft = new UnsignedLongFieldType(
            "my_unsigned_long",
            true,
            false,
            false,
            null,
            Collections.emptyMap(),
            false,
            null,
            null,
            false
        );

        assertEquals(
            LongPoint.newRangeQuery("my_unsigned_long", -9223372036854775808L, -9223372036854775808L),
            ft.rangeQuery(-1L, 0L, true, true, null)
        );
        assertEquals(
            LongPoint.newRangeQuery("my_unsigned_long", -9223372036854775808L, -9223372036854775808L),
            ft.rangeQuery(0.0, 0.5, true, true, null)
        );
        assertEquals(
            LongPoint.newRangeQuery("my_unsigned_long", 0, 0),
            ft.rangeQuery("9223372036854775807", "9223372036854775808", false, true, null)
        );
        assertEquals(
            LongPoint.newRangeQuery("my_unsigned_long", -9223372036854775808L, 9223372036854775806L),
            ft.rangeQuery(null, "18446744073709551614.5", true, true, null)
        );
        assertEquals(
            LongPoint.newRangeQuery("my_unsigned_long", 9223372036854775807L, 9223372036854775807L),
            ft.rangeQuery("18446744073709551615", "18446744073709551616", true, true, null)
        );

        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery(-1f, -0.5f, true, true, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery(-1L, 0L, true, false, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery(9223372036854775807L, 9223372036854775806L, true, true, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("18446744073709551616", "18446744073709551616", true, true, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("18446744073709551615", "18446744073709551616", false, true, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery(9223372036854775807L, 9223372036854775806L, true, true, null));

        expectThrows(NumberFormatException.class, () -> ft.rangeQuery("18incorrectnumber", "18incorrectnumber", true, true, null));
    }

    public void testParseTermForTermQuery() {
        // values that represent proper unsigned long number
        assertEquals(0L, parseTerm("0").longValue());
        assertEquals(0L, parseTerm(0).longValue());
        assertEquals(9223372036854775807L, parseTerm(9223372036854775807L).longValue());
        assertEquals(-1L, parseTerm("18446744073709551615").longValue());

        // values that represent numbers but not unsigned long and not in range of [0; 18446744073709551615]
        assertEquals(null, parseTerm("-9223372036854775808.05"));
        assertEquals(null, parseTerm(-9223372036854775808L));
        assertEquals(null, parseTerm(0.0));
        assertEquals(null, parseTerm(0.5));
        assertEquals(null, parseTerm("18446744073709551616"));

        // wrongly formatted numbers
        expectThrows(NumberFormatException.class, () -> parseTerm("18incorrectnumber"));
    }

    public void testParseLowerTermForRangeQuery() {
        // values that are lower than min for lowerTerm are converted to 0
        assertEquals(0L, parseLowerRangeTerm(-9223372036854775808L, true).longValue());
        assertEquals(0L, parseLowerRangeTerm("-9223372036854775808", true).longValue());
        assertEquals(0L, parseLowerRangeTerm("-1", true).longValue());
        assertEquals(0L, parseLowerRangeTerm("-0.5", true).longValue());

        assertEquals(0L, parseLowerRangeTerm(0L, true).longValue());
        assertEquals(0L, parseLowerRangeTerm("0", true).longValue());
        assertEquals(0L, parseLowerRangeTerm("0.0", true).longValue());
        assertEquals(1L, parseLowerRangeTerm("0", false).longValue());
        assertEquals(1L, parseLowerRangeTerm("0.5", true).longValue());
        assertEquals(9223372036854775807L, parseLowerRangeTerm(9223372036854775806L, false).longValue());
        assertEquals(9223372036854775807L, parseLowerRangeTerm(9223372036854775807L, true).longValue());
        assertEquals(-9223372036854775808L, parseLowerRangeTerm(9223372036854775807L, false).longValue());
        assertEquals(-1L, parseLowerRangeTerm("18446744073709551614", false).longValue());
        assertEquals(-1L, parseLowerRangeTerm("18446744073709551614.1", true).longValue());
        assertEquals(-1L, parseLowerRangeTerm("18446744073709551615", true).longValue());

        // values that are higher than max for lowerTerm don't return results
        assertEquals(null, parseLowerRangeTerm("18446744073709551615", false));
        assertEquals(null, parseLowerRangeTerm("18446744073709551616", true));

        // wrongly formatted numbers
        expectThrows(NumberFormatException.class, () -> parseLowerRangeTerm("18incorrectnumber", true));
    }

    public void testParseUpperTermForRangeQuery() {
        // values that are lower than min for upperTerm don't return results
        assertEquals(null, parseUpperRangeTerm(-9223372036854775808L, true));
        assertEquals(null, parseUpperRangeTerm("-1", true));
        assertEquals(null, parseUpperRangeTerm("-0.5", true));
        assertEquals(null, parseUpperRangeTerm(0L, false));

        assertEquals(0L, parseUpperRangeTerm(0L, true).longValue());
        assertEquals(0L, parseUpperRangeTerm("0", true).longValue());
        assertEquals(0L, parseUpperRangeTerm("0.0", true).longValue());
        assertEquals(0L, parseUpperRangeTerm("0.5", true).longValue());
        assertEquals(9223372036854775806L, parseUpperRangeTerm(9223372036854775807L, false).longValue());
        assertEquals(9223372036854775807L, parseUpperRangeTerm(9223372036854775807L, true).longValue());
        assertEquals(-2L, parseUpperRangeTerm("18446744073709551614.5", true).longValue());
        assertEquals(-2L, parseUpperRangeTerm("18446744073709551615", false).longValue());
        assertEquals(-1L, parseUpperRangeTerm("18446744073709551615", true).longValue());

        // values that are higher than max for upperTerm are converted to "18446744073709551615" or -1 in singed representation
        assertEquals(-1L, parseUpperRangeTerm("18446744073709551615.8", true).longValue());
        assertEquals(-1L, parseUpperRangeTerm("18446744073709551616", true).longValue());

        // wrongly formatted numbers
        expectThrows(NumberFormatException.class, () -> parseUpperRangeTerm("18incorrectnumber", true));
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new UnsignedLongFieldMapper.Builder("field", false, null, null, null).build(
            MapperBuilderContext.root(false, false)
        ).fieldType();
        assertEquals(List.of(0L), fetchSourceValue(mapper, 0L));
        assertEquals(List.of(9223372036854775807L), fetchSourceValue(mapper, 9223372036854775807L));
        assertEquals(List.of(BIGINTEGER_2_64_MINUS_ONE), fetchSourceValue(mapper, "18446744073709551615"));
        assertEquals(List.of(), fetchSourceValue(mapper, ""));

        MappedFieldType nullValueMapper = new UnsignedLongFieldMapper.Builder("field", false, null, null, null).nullValue(
            "18446744073709551615"
        ).build(MapperBuilderContext.root(false, false)).fieldType();
        assertEquals(List.of(BIGINTEGER_2_64_MINUS_ONE), fetchSourceValue(nullValueMapper, ""));
    }
}
