/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.ql.util.StringUtils.parseIntegral;
import static org.hamcrest.Matchers.equalTo;

public class NumericUtilsTests extends ESTestCase {

    public void testUnsignedLongAddExact() {
        assertThat(addExact("9223372036854775808", "0"), equalTo("9223372036854775808"));
        assertThat(addExact("9223372036854775807", "0"), equalTo("9223372036854775807"));
        assertThat(addExact("9223372036854775808", "1"), equalTo("9223372036854775809"));
        assertThat(addExact("9223372036854775807", "1"), equalTo("9223372036854775808"));

        assertThat(addExact("0", "0"), equalTo("0"));
        assertThat(addExact("1", "1"), equalTo("2"));

        assertThat(addExact("9223372036854775808", "9223372036854775807"), equalTo("18446744073709551615"));
        assertThat(addExact("9223372036854775807", "9223372036854775807"), equalTo("18446744073709551614"));
        assertThat(addExact("9223372036854775806", "9223372036854775807"), equalTo("18446744073709551613"));
        assertThat(addExact("9223372036854775805", "9223372036854775807"), equalTo("18446744073709551612"));

        assertThat(addExact("18446744073709551612", "3"), equalTo("18446744073709551615"));
        assertThat(addExact("18446744073709551613", "2"), equalTo("18446744073709551615"));
        assertThat(addExact("18446744073709551614", "1"), equalTo("18446744073709551615"));
        assertThat(addExact("18446744073709551615", "0"), equalTo("18446744073709551615"));

        expectThrows(ArithmeticException.class, () -> addExact("18446744073709551615", "1"));
        expectThrows(ArithmeticException.class, () -> addExact("18446744073709551615", "2"));
        expectThrows(ArithmeticException.class, () -> addExact("18446744073709551615", "3"));
        expectThrows(ArithmeticException.class, () -> addExact("18446744073709551614", "2"));
        expectThrows(ArithmeticException.class, () -> addExact("18446744073709551615", "18446744073709551615"));
        expectThrows(ArithmeticException.class, () -> addExact("18446744073709551615", "18446744073709551614"));
        expectThrows(ArithmeticException.class, () -> addExact("18446744073709551615", "9223372036854775808"));
        expectThrows(ArithmeticException.class, () -> addExact("18446744073709551615", "9223372036854775807"));
        expectThrows(ArithmeticException.class, () -> addExact("9223372036854775808", "9223372036854775808"));
        expectThrows(ArithmeticException.class, () -> addExact("9223372036854775807", "9223372036854775809"));
    }

    public void testUnsignedLongSubtractExact() {
        assertThat(subExact("18446744073709551615", "0"), equalTo("18446744073709551615"));
        assertThat(subExact("18446744073709551615", "18446744073709551615"), equalTo("0"));

        assertThat(subExact("18446744073709551615", "9223372036854775808"), equalTo("9223372036854775807"));
        assertThat(subExact("18446744073709551615", "9223372036854775807"), equalTo("9223372036854775808"));
        assertThat(subExact("18446744073709551615", "9223372036854775806"), equalTo("9223372036854775809"));
        assertThat(subExact("18446744073709551614", "9223372036854775808"), equalTo("9223372036854775806"));
        assertThat(subExact("18446744073709551614", "9223372036854775807"), equalTo("9223372036854775807"));

        assertThat(subExact("9223372036854775809", "9223372036854775809"), equalTo("0"));
        assertThat(subExact("9223372036854775808", "9223372036854775808"), equalTo("0"));

        assertThat(subExact("9223372036854775808", "1"), equalTo("9223372036854775807"));
        assertThat(subExact("9223372036854775807", "1"), equalTo("9223372036854775806"));
        assertThat(subExact("9223372036854775808", "0"), equalTo("9223372036854775808"));
        assertThat(subExact("9223372036854775807", "0"), equalTo("9223372036854775807"));

        assertThat(subExact("0", "0"), equalTo("0"));
        assertThat(subExact("1", "1"), equalTo("0"));

        expectThrows(ArithmeticException.class, () -> subExact("9223372036854775807", "9223372036854775808"));
        expectThrows(ArithmeticException.class, () -> subExact("9223372036854775805", "9223372036854775808"));
        expectThrows(ArithmeticException.class, () -> subExact("9223372036854775805", "9223372036854775806"));
        expectThrows(ArithmeticException.class, () -> subExact("0", "9223372036854775808"));
        expectThrows(ArithmeticException.class, () -> subExact("0", "9223372036854775807"));
        expectThrows(ArithmeticException.class, () -> subExact("0", "9223372036854775805"));
    }

    // 18446744073709551615 = 3 * 5 * 17 * 257 * 641 * 65537 * 6700417
    public void testUnsignedLongMultiplyExact() {
        assertThat(mulExact("6148914691236517205", "3"), equalTo("18446744073709551615"));
        expectThrows(ArithmeticException.class, () -> mulExact("6148914691236517205", "4"));
        expectThrows(ArithmeticException.class, () -> mulExact("6148914691236517206", "3"));

        assertThat(mulExact("3689348814741910323", "5"), equalTo("18446744073709551615"));
        expectThrows(ArithmeticException.class, () -> mulExact("3689348814741910324", "5"));
        expectThrows(ArithmeticException.class, () -> mulExact("3689348814741910323", "6"));

        assertThat(mulExact("6700417", "2753074036095"), equalTo("18446744073709551615"));
        expectThrows(ArithmeticException.class, () -> mulExact("6700418", "2753074036095"));
        expectThrows(ArithmeticException.class, () -> mulExact("6700417", "2753074036096"));

        assertThat(mulExact("1844674407370955161", "0"), equalTo("0"));
        assertThat(mulExact("1844674407370955161", "9"), equalTo("16602069666338596449"));
        assertThat(mulExact("1844674407370955161", "10"), equalTo("18446744073709551610"));
        expectThrows(ArithmeticException.class, () -> mulExact("1844674407370955161", "11"));

        assertThat(mulExact("18446744073709551615", "1"), equalTo("18446744073709551615"));
        expectThrows(ArithmeticException.class, () -> mulExact("18446744073709551615", "2"));
        expectThrows(ArithmeticException.class, () -> mulExact("18446744073709551615", "10"));
        expectThrows(ArithmeticException.class, () -> mulExact("18446744073709551615", "18446744073709551615"));

        assertThat(mulExact("9223372036854775807", "2"), equalTo("18446744073709551614"));
        expectThrows(ArithmeticException.class, () -> mulExact("9223372036854775808", "2"));
        expectThrows(ArithmeticException.class, () -> mulExact("9223372036854775807", "3"));
        expectThrows(ArithmeticException.class, () -> mulExact("9223372036854775808", "9223372036854775808"));
        expectThrows(ArithmeticException.class, () -> mulExact("9223372036854775807", "9223372036854775807"));
        expectThrows(ArithmeticException.class, () -> mulExact("9223372036854775807", "9223372036854775808"));

        assertThat(mulExact("1", "1"), equalTo("1"));
        assertThat(mulExact("0", "1"), equalTo("0"));
        assertThat(mulExact("0", "0"), equalTo("0"));
    }

    private static String addExact(String x, String y) {
        return exactOperation(x, y, NumericUtils::unsignedLongAddExact);
    }

    private static String subExact(String x, String y) {
        return exactOperation(x, y, NumericUtils::unsignedLongSubtractExact);
    }

    private static String mulExact(String x, String y) {
        return exactOperation(x, y, NumericUtils::unsignedLongMultiplyExact);
    }

    private static String exactOperation(String x, String y, BiFunction<Long, Long, Long> operation) {
        long xl = parseUnsignedLong(x);
        long yl = parseUnsignedLong(y);
        long rl = operation.apply(xl, yl);
        return unsignedLongAsNumber(rl).toString();
    }

    private static long parseUnsignedLong(String number) {
        Number n = parseIntegral(number);
        return n instanceof BigInteger bi ? asLongUnsigned(bi) : asLongUnsigned(n.longValue());
    }
}
