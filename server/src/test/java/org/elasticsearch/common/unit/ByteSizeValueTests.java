/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.function.Function;

import static java.lang.Math.multiplyExact;
import static org.elasticsearch.common.unit.ByteSizeUnit.BYTES;
import static org.elasticsearch.common.unit.ByteSizeUnit.GB;
import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.common.unit.ByteSizeUnit.PB;
import static org.elasticsearch.common.unit.ByteSizeUnit.TB;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ByteSizeValueTests extends AbstractWireSerializingTestCase<ByteSizeValue> {
    public void testActualPeta() {
        MatcherAssert.assertThat(ByteSizeValue.of(4, PB).getBytes(), equalTo(4503599627370496L));
    }

    public void testActualTera() {
        MatcherAssert.assertThat(ByteSizeValue.of(4, TB).getBytes(), equalTo(4398046511104L));
    }

    public void testActual() {
        MatcherAssert.assertThat(ByteSizeValue.of(4, GB).getBytes(), equalTo(4294967296L));
    }

    public void testSimple() {
        assertThat(ByteSizeUnit.BYTES.toBytes(10), is(ByteSizeValue.of(10, ByteSizeUnit.BYTES).getBytes()));
        assertThat(ByteSizeUnit.KB.toKB(10), is(ByteSizeValue.of(10, ByteSizeUnit.KB).getKb()));
        assertThat(MB.toMB(10), is(ByteSizeValue.of(10, MB).getMb()));
        assertThat(GB.toGB(10), is(ByteSizeValue.of(10, GB).getGb()));
        assertThat(TB.toTB(10), is(ByteSizeValue.of(10, TB).getTb()));
        assertThat(PB.toPB(10), is(ByteSizeValue.of(10, PB).getPb()));
    }

    public void testToIntBytes() {
        assertThat(ByteSizeUnit.BYTES.toIntBytes(4), equalTo(4));
        assertThat(ByteSizeUnit.KB.toIntBytes(4), equalTo(4096));
        assertThat(
            expectThrows(AssertionError.class, () -> GB.toIntBytes(4)).getMessage(),
            containsString("could not convert [4 GB] to an int")
        );
    }

    public void testEquality() {
        String[] equalValues = new String[] { "1GB", "1024MB", "1048576KB", "1073741824B" };
        for (int first = 0; first < equalValues.length; first++) {
            ByteSizeValue firstValue = ByteSizeValue.parseBytesSizeValue(equalValues[first], "equalTest");
            for (int second = first + 1; second < equalValues.length; second++) {
                ByteSizeValue secondValue = ByteSizeValue.parseBytesSizeValue(equalValues[second], "equalTest");
                assertEquals(firstValue, secondValue);
            }
        }
    }

    public void testToString() {
        assertThat("10b", is(ByteSizeValue.of(10, ByteSizeUnit.BYTES).toString()));
        assertThat("1.5kb", is(ByteSizeValue.of((long) (1024 * 1.5), ByteSizeUnit.BYTES).toString()));
        assertThat("1.5mb", is(ByteSizeValue.of((long) (1024 * 1.5), ByteSizeUnit.KB).toString()));
        assertThat("1.5gb", is(ByteSizeValue.of((long) (1024 * 1.5), ByteSizeUnit.MB).toString()));
        assertThat("1.5tb", is(ByteSizeValue.of((long) (1024 * 1.5), ByteSizeUnit.GB).toString()));
        assertThat("1.5pb", is(ByteSizeValue.of((long) (1024 * 1.5), ByteSizeUnit.TB).toString()));
        assertThat("1536pb", is(ByteSizeValue.of((long) (1024 * 1.5), PB).toString()));
    }

    public void testParsing() {
        assertThat(ByteSizeValue.parseBytesSizeValue("42PB", "testParsing").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42 PB", "testParsing").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42pb", "testParsing").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42 pb", "testParsing").toString(), is("42pb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("42P", "testParsing").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42 P", "testParsing").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42p", "testParsing").toString(), is("42pb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("42 p", "testParsing").toString(), is("42pb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("54TB", "testParsing").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54 TB", "testParsing").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54tb", "testParsing").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54 tb", "testParsing").toString(), is("54tb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("54T", "testParsing").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54 T", "testParsing").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54t", "testParsing").toString(), is("54tb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("54 t", "testParsing").toString(), is("54tb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("12GB", "testParsing").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12 GB", "testParsing").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12gb", "testParsing").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12 gb", "testParsing").toString(), is("12gb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("12G", "testParsing").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12 G", "testParsing").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12g", "testParsing").toString(), is("12gb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12 g", "testParsing").toString(), is("12gb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("12M", "testParsing").toString(), is("12mb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12 M", "testParsing").toString(), is("12mb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12m", "testParsing").toString(), is("12mb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("12 m", "testParsing").toString(), is("12mb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("23KB", "testParsing").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23 KB", "testParsing").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23kb", "testParsing").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23 kb", "testParsing").toString(), is("23kb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("23K", "testParsing").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23 K", "testParsing").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23k", "testParsing").toString(), is("23kb"));
        assertThat(ByteSizeValue.parseBytesSizeValue("23 k", "testParsing").toString(), is("23kb"));

        assertThat(ByteSizeValue.parseBytesSizeValue("1B", "testParsing").toString(), is("1b"));
        assertThat(ByteSizeValue.parseBytesSizeValue("1 B", "testParsing").toString(), is("1b"));
        assertThat(ByteSizeValue.parseBytesSizeValue("1b", "testParsing").toString(), is("1b"));
        assertThat(ByteSizeValue.parseBytesSizeValue("1 b", "testParsing").toString(), is("1b"));
    }

    public void testFailOnMissingUnits() {
        Exception e = expectThrows(ElasticsearchParseException.class, () -> ByteSizeValue.parseBytesSizeValue("23", "test"));
        assertThat(e.getMessage(), containsString("failed to parse setting [test]"));
    }

    public void testFailOnUnknownUnits() {
        Exception e = expectThrows(ElasticsearchParseException.class, () -> ByteSizeValue.parseBytesSizeValue("23jw", "test"));
        assertThat(e.getMessage(), containsString("failed to parse setting [test]"));
    }

    public void testFailOnEmptyParsing() {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> assertThat(ByteSizeValue.parseBytesSizeValue("", "emptyParsing").toString(), is("23kb"))
        );
        assertThat(e.getMessage(), containsString("failed to parse setting [emptyParsing]"));
    }

    public void testFailOnEmptyNumberParsing() {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> assertThat(ByteSizeValue.parseBytesSizeValue("g", "emptyNumberParsing").toString(), is("23b"))
        );
        assertThat(e.getMessage(), containsString("failed to parse setting [emptyNumberParsing] with value [g]"));
    }

    public void testNoDotsAllowed() {
        Exception e = expectThrows(ElasticsearchParseException.class, () -> ByteSizeValue.parseBytesSizeValue("42b.", null, "test"));
        assertThat(e.getMessage(), containsString("failed to parse setting [test]"));
    }

    public void testCompareEquality() {
        ByteSizeUnit randomUnit = randomFrom(ByteSizeUnit.values());
        long firstRandom = randomNonNegativeLong() / randomUnit.toBytes(1);
        ByteSizeValue firstByteValue = ByteSizeValue.of(firstRandom, randomUnit);
        ByteSizeValue secondByteValue = ByteSizeValue.of(firstRandom, randomUnit);
        assertEquals(0, firstByteValue.compareTo(secondByteValue));
    }

    public void testCompareValue() {
        ByteSizeUnit unit = randomFrom(ByteSizeUnit.values());
        long firstRandom = randomNonNegativeLong() / unit.toBytes(1);
        long secondRandom = randomValueOtherThan(firstRandom, () -> randomNonNegativeLong() / unit.toBytes(1));
        ByteSizeValue firstByteValue = ByteSizeValue.of(firstRandom, unit);
        ByteSizeValue secondByteValue = ByteSizeValue.of(secondRandom, unit);
        assertEquals(firstRandom > secondRandom, firstByteValue.compareTo(secondByteValue) > 0);
        assertEquals(secondRandom > firstRandom, secondByteValue.compareTo(firstByteValue) > 0);
    }

    public void testCompareUnits() {
        long number = randomLongBetween(1, Long.MAX_VALUE / PB.toBytes(1));
        ByteSizeUnit randomUnit = randomValueOtherThan(PB, () -> randomFrom(ByteSizeUnit.values()));
        ByteSizeValue firstByteValue = ByteSizeValue.of(number, randomUnit);
        ByteSizeValue secondByteValue = ByteSizeValue.of(number, PB);
        assertTrue(firstByteValue.compareTo(secondByteValue) < 0);
        assertTrue(secondByteValue.compareTo(firstByteValue) > 0);
    }

    public void testOutOfRange() {
        // Make sure a value of > Long.MAX_VALUE bytes throws an exception
        ByteSizeUnit unit = randomValueOtherThan(ByteSizeUnit.BYTES, () -> randomFrom(ByteSizeUnit.values()));
        long size = (long) randomDouble() * unit.toBytes(1) + (Long.MAX_VALUE - unit.toBytes(1));
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.of(size, unit));
        assertEquals(
            "Values greater than " + Long.MAX_VALUE + " bytes are not supported: " + size + unit.getSuffix(),
            exception.getMessage()
        );

        // Make sure for units other than BYTES a size of -1 throws an exception
        ByteSizeUnit unit2 = randomValueOtherThan(ByteSizeUnit.BYTES, () -> randomFrom(ByteSizeUnit.values()));
        long size2 = -1L;
        exception = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.of(size2, unit2));
        assertEquals("Values less than -1 bytes are not supported: " + size2 + unit2.getSuffix(), exception.getMessage());

        // Make sure for any unit a size < -1 throws an exception
        ByteSizeUnit unit3 = randomFrom(ByteSizeUnit.values());
        long size3 = -1L * randomNonNegativeLong() - 1L;
        exception = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.of(size3, unit3));
        assertEquals("Values less than -1 bytes are not supported: " + size3 + unit3.getSuffix(), exception.getMessage());
    }

    public void testConversionHashCode() {
        ByteSizeValue firstValue = ByteSizeValue.of(randomIntBetween(0, Integer.MAX_VALUE), GB);
        ByteSizeValue secondValue = ByteSizeValue.of(firstValue.getBytes(), ByteSizeUnit.BYTES);
        assertEquals(firstValue.hashCode(), secondValue.hashCode());
    }

    @Override
    protected ByteSizeValue createTestInstance() {
        if (randomBoolean()) {
            ByteSizeUnit unit = randomFrom(ByteSizeUnit.values());
            long size = randomNonNegativeLong() / unit.toBytes(1);
            if (size > Long.MAX_VALUE / unit.toBytes(1)) {
                throw new AssertionError();
            }
            return ByteSizeValue.of(size, unit);
        } else {
            return ByteSizeValue.ofBytes(randomNonNegativeLong());
        }
    }

    @Override
    protected Reader<ByteSizeValue> instanceReader() {
        return ByteSizeValue::readFrom;
    }

    @Override
    protected ByteSizeValue mutateInstance(final ByteSizeValue instance) {
        // There's just one way to create an unequal ByteSizeValue: change the size in bytes.
        // Changing only the unit does not make the ByteSizeValue unequal.
        return new ByteSizeValue(
            randomValueOtherThan(instance.sizeInBytes, ESTestCase::randomNonNegativeLong),
            instance.preferredUnit,
            0xdead
        );
    }

    public void testParse() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            ByteSizeValue original = createTestInstance();
            String serialised = original.getStringRep();
            ByteSizeValue copy = ByteSizeValue.parseBytesSizeValue(serialised, "test");
            assertEquals(original, copy);
            assertEquals(serialised, copy.getStringRep());
        }
    }

    public void testParseInvalidValue() {
        String unitSuffix = (randomBoolean() ? " " : "") + randomFrom(ByteSizeUnit.values()).getSuffix();
        ElasticsearchParseException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> ByteSizeValue.parseBytesSizeValue("-6" + unitSuffix, "test_setting")
        );
        assertEquals("failed to parse setting [test_setting] with value [-6" + unitSuffix + "] as a size in bytes", exception.getMessage());
        assertNotNull(exception.getCause());
        assertEquals(IllegalArgumentException.class, exception.getCause().getClass());
    }

    public void testParseDefaultValue() {
        ByteSizeValue defaultValue = createTestInstance();
        assertEquals(defaultValue, ByteSizeValue.parseBytesSizeValue(null, defaultValue, "test"));
    }

    public void testParseSpecialValues() throws IOException {
        ByteSizeValue instance = ByteSizeValue.ofBytes(-1);
        assertEquals(instance, ByteSizeValue.parseBytesSizeValue(instance.getStringRep(), null, "test"));
        assertSerialization(instance);

        instance = ByteSizeValue.ofBytes(0);
        assertEquals(instance, ByteSizeValue.parseBytesSizeValue(instance.getStringRep(), null, "test"));
        assertSerialization(instance);
    }

    public void testParseInvalidNumber() throws IOException {
        ElasticsearchParseException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> ByteSizeValue.parseBytesSizeValue("notANumber", "test")
        );
        assertEquals(
            "failed to parse setting [test] with value [notANumber] as a size in bytes: unit is missing or unrecognized",
            exception.getMessage()
        );

        ByteSizeUnit unit = randomFrom(ByteSizeUnit.values());
        String unitSuffix = (randomBoolean() ? " " : "") + unit.getSuffix();
        exception = expectThrows(
            ElasticsearchParseException.class,
            () -> ByteSizeValue.parseBytesSizeValue("notANumber" + unitSuffix, "test")
        );
        assertEquals("failed to parse setting [test] with value [notANumber" + unitSuffix + "]", exception.getMessage());

        exception = expectThrows(ElasticsearchParseException.class, () -> ByteSizeValue.parseBytesSizeValue("1.1b", "test"));
        assertEquals(
            "No decimal places allowed if the unit is bytes",
            "failed to parse setting [test] with value [1.1b]",
            exception.getMessage()
        );

        exception = expectThrows(ElasticsearchParseException.class, () -> ByteSizeValue.parseBytesSizeValue("1.234" + unitSuffix, "test"));
        if (unit == BYTES) {
            // Decimals aren't allowed at all for bytes
            assertEquals("failed to parse setting [test] with value [1.234" + unitSuffix + "]", exception.getMessage());
        } else {
            assertEquals("more than two decimal places in setting [test] with value [1.234" + unitSuffix + "]", exception.getMessage());
        }
    }

    public void testParseFractionalNumber() throws IOException {
        ByteSizeUnit unit = randomValueOtherThan(ByteSizeUnit.BYTES, () -> randomFrom(ByteSizeUnit.values()));
        String fractionalValue = "23.5" + unit.getSuffix();
        ByteSizeValue instance = ByteSizeValue.parseBytesSizeValue(fractionalValue, "test");
        assertEquals(fractionalValue, instance.toString());
    }

    @SuppressWarnings("deprecation") // bytesAsInt is deprecated
    public void testGetBytesAsInt() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            ByteSizeValue instance = ByteSizeValue.of(randomIntBetween(1, 1000), randomFrom(ByteSizeUnit.values()));
            long bytesValue = instance.getBytes();
            if (bytesValue > Integer.MAX_VALUE) {
                IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> instance.bytesAsInt());
                assertEquals("size [" + instance + "] is bigger than max int", exception.getMessage());
            } else {
                assertEquals((int) bytesValue, instance.bytesAsInt());
            }
        }
    }

    public void testOfBytes() {
        testOf(ByteSizeUnit.BYTES, ByteSizeValue::ofBytes);
    }

    public void testOfKb() {
        testOf(ByteSizeUnit.KB, ByteSizeValue::ofKb);
    }

    public void testOfMb() {
        testOf(MB, ByteSizeValue::ofMb);
    }

    public void testOfGb() {
        testOf(GB, ByteSizeValue::ofGb);
    }

    public void testOfTb() {
        testOf(TB, ByteSizeValue::ofTb);
    }

    public void testOfPb() {
        testOf(PB, ByteSizeValue::ofPb);
    }

    private void testOf(ByteSizeUnit unit, Function<Long, ByteSizeValue> byteSizeValueFunction) {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            long size = randomIntBetween(1, 1000);
            ByteSizeValue expected = ByteSizeValue.of(size, unit);
            ByteSizeValue actual = byteSizeValueFunction.apply(size);
            assertThat(actual, equalTo(expected));
        }
    }

    public void testAddition() {
        assertThat(ByteSizeValue.add(ByteSizeValue.ZERO, ByteSizeValue.ZERO), is(ByteSizeValue.ZERO));
        assertThat(ByteSizeValue.add(ByteSizeValue.ZERO, ByteSizeValue.ONE), is(ByteSizeValue.ONE));
        assertThat(ByteSizeValue.add(ByteSizeValue.ONE, ByteSizeValue.ONE), is(ByteSizeValue.ofBytes(2L)));
        assertThat(ByteSizeValue.add(ByteSizeValue.ofBytes(100L), ByteSizeValue.ONE), is(ByteSizeValue.ofBytes(101L)));
        assertThat(ByteSizeValue.add(ByteSizeValue.ofBytes(100L), ByteSizeValue.ofBytes(2L)), is(ByteSizeValue.ofBytes(102L)));
        assertThat(
            ByteSizeValue.add(ByteSizeValue.of(8, ByteSizeUnit.KB), ByteSizeValue.of(4, ByteSizeUnit.KB)),
            is(ByteSizeValue.ofBytes(12288L))
        );
        assertThat(ByteSizeValue.add(ByteSizeValue.of(8, MB), ByteSizeValue.of(4, MB)), is(ByteSizeValue.ofBytes(12582912L)));
        assertThat(ByteSizeValue.add(ByteSizeValue.of(8, GB), ByteSizeValue.of(4, GB)), is(ByteSizeValue.ofBytes(12884901888L)));
        assertThat(ByteSizeValue.add(ByteSizeValue.of(8, TB), ByteSizeValue.of(4, TB)), is(ByteSizeValue.ofBytes(13194139533312L)));
        assertThat(ByteSizeValue.add(ByteSizeValue.of(8, PB), ByteSizeValue.of(4, PB)), is(ByteSizeValue.ofBytes(13510798882111488L)));
        assertThat(ByteSizeValue.add(ByteSizeValue.of(8, PB), ByteSizeValue.of(4, GB)), is(ByteSizeValue.ofBytes(9007203549708288L)));

        Exception e = expectThrows(
            ArithmeticException.class,
            () -> ByteSizeValue.add(ByteSizeValue.ofBytes(Long.MAX_VALUE), ByteSizeValue.ONE)
        );
        assertThat(e.getMessage(), containsString("long overflow"));

        String exceptionMessage = "one of the arguments has -1 bytes";
        e = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.add(ByteSizeValue.MINUS_ONE, ByteSizeValue.ONE));
        assertThat(e.getMessage(), containsString(exceptionMessage));

        e = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.add(ByteSizeValue.ZERO, ByteSizeValue.MINUS_ONE));
        assertThat(e.getMessage(), containsString(exceptionMessage));

        e = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.add(ByteSizeValue.MINUS_ONE, ByteSizeValue.MINUS_ONE));
        assertThat(e.getMessage(), containsString(exceptionMessage));
    }

    public void testSubtraction() {
        assertThat(ByteSizeValue.subtract(ByteSizeValue.ZERO, ByteSizeValue.ZERO), is(ByteSizeValue.ZERO));
        assertThat(ByteSizeValue.subtract(ByteSizeValue.ONE, ByteSizeValue.ZERO), is(ByteSizeValue.ONE));
        assertThat(ByteSizeValue.subtract(ByteSizeValue.ONE, ByteSizeValue.ONE), is(ByteSizeValue.ZERO));
        assertThat(ByteSizeValue.subtract(ByteSizeValue.ofBytes(100L), ByteSizeValue.ONE), is(ByteSizeValue.ofBytes(99L)));
        assertThat(ByteSizeValue.subtract(ByteSizeValue.ofBytes(100L), ByteSizeValue.ofBytes(2L)), is(ByteSizeValue.ofBytes(98L)));
        assertThat(
            ByteSizeValue.subtract(ByteSizeValue.of(8, ByteSizeUnit.KB), ByteSizeValue.of(4, ByteSizeUnit.KB)),
            is(ByteSizeValue.ofBytes(4096L))
        );
        assertThat(ByteSizeValue.subtract(ByteSizeValue.of(8, MB), ByteSizeValue.of(4, MB)), is(ByteSizeValue.ofBytes(4194304L)));
        assertThat(ByteSizeValue.subtract(ByteSizeValue.of(8, GB), ByteSizeValue.of(4, GB)), is(ByteSizeValue.ofBytes(4294967296L)));
        assertThat(ByteSizeValue.subtract(ByteSizeValue.of(8, TB), ByteSizeValue.of(4, TB)), is(ByteSizeValue.ofBytes(4398046511104L)));
        assertThat(ByteSizeValue.subtract(ByteSizeValue.of(8, PB), ByteSizeValue.of(4, PB)), is(ByteSizeValue.ofBytes(4503599627370496L)));
        assertThat(ByteSizeValue.subtract(ByteSizeValue.of(8, PB), ByteSizeValue.of(4, GB)), is(ByteSizeValue.ofBytes(9007194959773696L)));

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> ByteSizeValue.subtract(ByteSizeValue.ofBytes(100L), ByteSizeValue.ofBytes(102L))
        );
        assertThat(e.getMessage(), containsString("Values less than -1 bytes are not supported: -2b"));

        e = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.subtract(ByteSizeValue.ZERO, ByteSizeValue.ONE));
        assertThat(e.getMessage(), containsString("subtraction result has -1 bytes"));

        String exceptionMessage = "one of the arguments has -1 bytes";
        e = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.subtract(ByteSizeValue.MINUS_ONE, ByteSizeValue.ONE));
        assertThat(e.getMessage(), containsString(exceptionMessage));

        e = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.subtract(ByteSizeValue.ZERO, ByteSizeValue.MINUS_ONE));
        assertThat(e.getMessage(), containsString(exceptionMessage));

        e = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.subtract(ByteSizeValue.MINUS_ONE, ByteSizeValue.MINUS_ONE));
        assertThat(e.getMessage(), containsString(exceptionMessage));
    }

    public void testMinimum() {
        assertThat(ByteSizeValue.min(ByteSizeValue.ZERO, ByteSizeValue.ZERO), is(ByteSizeValue.ZERO));
        assertThat(ByteSizeValue.min(ByteSizeValue.ZERO, ByteSizeValue.ONE), is(ByteSizeValue.ZERO));
        assertThat(ByteSizeValue.min(ByteSizeValue.ONE, ByteSizeValue.ZERO), is(ByteSizeValue.ZERO));
        assertThat(ByteSizeValue.min(ByteSizeValue.ONE, ByteSizeValue.ONE), is(ByteSizeValue.ONE));
        assertThat(ByteSizeValue.min(ByteSizeValue.ofBytes(100L), ByteSizeValue.ONE), is(ByteSizeValue.ONE));
        assertThat(ByteSizeValue.min(ByteSizeValue.ONE, ByteSizeValue.ofBytes(100L)), is(ByteSizeValue.ONE));
        assertThat(ByteSizeValue.min(ByteSizeValue.ofBytes(100L), ByteSizeValue.ofBytes(2L)), is(ByteSizeValue.ofBytes(2L)));
        assertThat(ByteSizeValue.min(ByteSizeValue.ofBytes(2L), ByteSizeValue.ofBytes(100L)), is(ByteSizeValue.ofBytes(2L)));

        assertThat(
            ByteSizeValue.min(ByteSizeValue.of(8, ByteSizeUnit.KB), ByteSizeValue.of(4, ByteSizeUnit.KB)),
            is(ByteSizeValue.of(4, ByteSizeUnit.KB))
        );
        assertThat(ByteSizeValue.min(ByteSizeValue.of(4, MB), ByteSizeValue.of(8, MB)), is(ByteSizeValue.of(4, MB)));
        assertThat(ByteSizeValue.min(ByteSizeValue.of(16, GB), ByteSizeValue.of(15, GB)), is(ByteSizeValue.of(15, GB)));
        assertThat(ByteSizeValue.min(ByteSizeValue.of(90, TB), ByteSizeValue.of(91, TB)), is(ByteSizeValue.of(90, TB)));
        assertThat(ByteSizeValue.min(ByteSizeValue.of(2, PB), ByteSizeValue.of(1, PB)), is(ByteSizeValue.of(1, PB)));
        assertThat(ByteSizeValue.min(ByteSizeValue.of(1, PB), ByteSizeValue.of(1, GB)), is(ByteSizeValue.of(1, GB)));

        ByteSizeValue equalityResult = ByteSizeValue.min(ByteSizeValue.of(1024, MB), ByteSizeValue.of(1, GB));
        assertThat(equalityResult, is(ByteSizeValue.of(1024, MB)));
        assertThat(equalityResult.preferredUnit, is(MB));

        equalityResult = ByteSizeValue.min(ByteSizeValue.of(1, GB), ByteSizeValue.of(1024, MB));
        assertThat(equalityResult, is(ByteSizeValue.of(1, GB)));
        assertThat(equalityResult.preferredUnit, is(GB));

        String exceptionMessage = "one of the arguments has -1 bytes";
        Exception e = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.min(ByteSizeValue.MINUS_ONE, ByteSizeValue.ONE));
        assertThat(e.getMessage(), containsString(exceptionMessage));

        e = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.min(ByteSizeValue.ONE, ByteSizeValue.MINUS_ONE));
        assertThat(e.getMessage(), containsString(exceptionMessage));

        e = expectThrows(IllegalArgumentException.class, () -> ByteSizeValue.min(ByteSizeValue.MINUS_ONE, ByteSizeValue.MINUS_ONE));
        assertThat(e.getMessage(), containsString(exceptionMessage));
    }

    public void testParseFractionsExhaustively() {
        for (var unit : ByteSizeUnit.values()) {
            if (unit == BYTES) {
                // Can't specify fractions of a byte
                continue;
            }
            var granularity = unit.toBytes(1);
            long wholeUnits = Long.MAX_VALUE / granularity - 1; // Try to provoke a failure by exceeding double precision
            for (var percent = 0; percent < 100; percent++) {
                String stringToParse = wholeUnits + percentToDecimal(percent) + unit.getSuffix();
                ByteSizeValue expected = new ByteSizeValue(
                    wholeUnits * granularity + multiplyExact(granularity, percent) / 100,
                    unit,
                    0xdead
                );
                ByteSizeValue parsedValue = ByteSizeValue.parseBytesSizeValue(stringToParse, "test");
                assertEquals("Should parse correctly: " + stringToParse, expected, parsedValue);
            }
        }
    }

    private static String percentToDecimal(int percent) {
        if (percent <= 9) {
            return ".0" + percent;
        } else if (percent % 10 == 0) {
            return "." + percent / 10;
        } else {
            return "." + percent;
        }
    }

    public void testWithAutomaticUnit() {
        for (var unit : ByteSizeUnit.values()) {
            if (unit == BYTES) {
                continue;
            }
            long bytes = unit.toBytes(1);
            for (int numFourths = 1; numFourths <= 8; numFourths++) {
                checkAutomaticUnitWithCornerCases(bytes * numFourths / 4, unit);
            }
        }
    }

    private void checkAutomaticUnitWithCornerCases(long sizeInBytes, ByteSizeUnit expectedUnit) {
        assertEquals(
            sizeInBytes + " should use " + expectedUnit,
            new ByteSizeValue(sizeInBytes, expectedUnit, 0xdead),
            ByteSizeValue.withAutomaticUnit(sizeInBytes)
        );
        // Plus or minus one byte, it should revert to bytes
        assertEquals(
            sizeInBytes - 1 + " should use " + BYTES,
            new ByteSizeValue(sizeInBytes - 1, BYTES, 0xdead),
            ByteSizeValue.withAutomaticUnit(sizeInBytes - 1)
        );
        assertEquals(
            sizeInBytes + 1 + " should use " + BYTES,
            new ByteSizeValue(sizeInBytes + 1, BYTES, 0xdead),
            ByteSizeValue.withAutomaticUnit(sizeInBytes + 1)
        );
    }

    @Override
    protected void assertEqualInstances(ByteSizeValue expectedInstance, ByteSizeValue newInstance) {
        assertThat(newInstance, equalTo(expectedInstance));
        assertThat(newInstance.hashCode(), equalTo(expectedInstance.hashCode()));
    }
}
