package org.elasticsearch;

import org.elasticsearch.test.ElasticsearchTestCase;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Exceptions}.
 */
public class ExceptionsTests extends ElasticsearchTestCase {
    /**
     * Enables checking of expected exceptions including message.
     */
    @Rule
    public final ExpectedException expected = ExpectedException.none();

    /**
     * The message expected in exceptions.
     */
    private final String message = "message";
    /**
     * Message to indicate an unexpected test failure.
     */
    private final String unexpected = "Not expected";

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNull_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNull(null, message);
    }

    /**
     * Non-{@code null} should not trigger anything.
     */
    @Test
    public void ifNull_valid() {
        Exceptions.ifNull(new Object(), unexpected);
    }

    /**
     * Non-{@code null} should return the exact same value.
     */
    @Test
    public void ifNull_valid_same() {
        Object value = new Object();

        assertSame(value, Exceptions.ifNull(value, unexpected));
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifEmpty_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifEmpty(null, message);
    }

    /**
     * {@link String#isEmpty() Empty} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifEmpty_empty_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifEmpty("", message);
    }

    /**
     * Non-{@code null} and not {@link String#isEmpty() Empty} should not
     * trigger anything.
     */
    @Test
    public void ifEmpty_valid() {
        Exceptions.ifEmpty(" ", unexpected);
        Exceptions.ifEmpty("\n", unexpected);
        Exceptions.ifEmpty("\t", unexpected);
        Exceptions.ifEmpty("\r", unexpected);
        Exceptions.ifEmpty("not", unexpected);
        Exceptions.ifEmpty("not empty", unexpected);

        String value = " same  value ";

        assertSame(value, Exceptions.ifEmpty(value, unexpected));
    }

    /**
     * Non-{@code null} and not {@link String#isEmpty() Empty} should return the
     * exact same value that it was given.
     */
    @Test
    public void ifEmpty_valid_same() {
        String value = " same  value ";

        assertSame(value, Exceptions.ifNotHasText(value, unexpected));
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotHasText_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotHasText(null, message);
    }

    /**
     * {@link String#isEmpty() Empty} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotHasText_empty_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotHasText("", message);
    }

    /**
     * Blank {@link String}s should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotHasText_blank_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotHasText(" \t \r \n ", message);
    }

    /**
     * Non-{@code null}, not {@link String#isEmpty() Empty} and not blank should
     * not trigger anything.
     */
    @Test
    public void ifNotHasText_valid() {
        Exceptions.ifNotHasText("not blank", unexpected);
        Exceptions.ifNotHasText(" not  blank ", unexpected);
    }

    /**
     * Non-{@code null}, not {@link String#isEmpty() Empty} and not blank should
     * return the exact same value that it was given.
     */
    @Test
    public void ifNotHasText_valid_same() {
        String value = " same  value ";

        assertSame(value, Exceptions.ifNotHasText(value, unexpected));
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNegativeNumber_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNegative((Double) null, message);
    }

    /**
     * A negative number should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNegativeNumber_negative_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNegative(-Double.MIN_VALUE, message);
    }

    /**
     * A negative number should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNegativeNumber_invalid_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNegative(randomIntBetween(Integer.MIN_VALUE, -1), message);
    }

    /**
     * Non-negative numbers should not trigger anything.
     */
    @Test
    public void ifNegativeNumber_valid() {
        Exceptions.ifNegative(0, unexpected);
        Exceptions.ifNegative(0L, unexpected);
        Exceptions.ifNegative(0f, unexpected);
        Exceptions.ifNegative(0d, unexpected);
        Exceptions.ifNegative(1234, unexpected);
    }

    /**
     * Non-negative numbers should return the exact same value that it was
     * given.
     */
    @Test
    public void ifNegativeNumber_valid_same() {
        Integer value = randomIntBetween(0, Integer.MAX_VALUE);

        assertSame(value, Exceptions.ifNegative(value, unexpected));
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNegativeBigDecimal_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNegative((BigDecimal)null, message);
    }

    /**
     * A negative {@link BigDecimal} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNegativeBigDecimal_negative_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        BigDecimal smallNegative = BigDecimal.valueOf(-Double.MIN_VALUE);

        Exceptions.ifNegative(smallNegative, message);
    }

    /**
     * A negative {@link BigDecimal} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNegativeBigDecimal_invalid_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        BigDecimal value =
                BigDecimal.valueOf(randomIntBetween(Integer.MIN_VALUE, -1));

        Exceptions.ifNegative(value, message);
    }

    /**
     * Non-negative {@link BigDecimal}s should not trigger anything.
     */
    @Test
    public void ifNegativeBigDecimal_valid() {
        Exceptions.ifNegative(BigDecimal.ZERO, unexpected);
        Exceptions.ifNegative(BigDecimal.ONE, unexpected);
        Exceptions.ifNegative(BigDecimal.TEN, unexpected);
    }

    /**
     * Non-negative {@link BigDecimal}s should return the exact same value that
     * it was given.
     */
    @Test
    public void ifNegativeBigDecimal_valid_same() {
        BigDecimal value =
                BigDecimal.valueOf(randomIntBetween(0, Integer.MAX_VALUE));

        assertSame(value, Exceptions.ifNegative(value, unexpected));
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNegativeBigInteger_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNegative((BigInteger)null, message);
    }

    /**
     * A negative {@link BigInteger} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNegativeBigInteger_negative_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNegative(BigInteger.valueOf(-1), message);
    }

    /**
     * A negative {@link BigInteger} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNegativeBigInteger_invalid_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        BigInteger value =
                BigInteger.valueOf(randomIntBetween(Integer.MIN_VALUE, -1));

        Exceptions.ifNegative(value, message);
    }

    /**
     * Non-negative {@link BigInteger}s should not trigger anything.
     */
    @Test
    public void ifNegativeBigInteger_valid() {
        Exceptions.ifNegative(BigInteger.ZERO, unexpected);
        Exceptions.ifNegative(BigInteger.ONE, unexpected);
        Exceptions.ifNegative(BigInteger.TEN, unexpected);
    }

    /**
     * Non-negative {@link BigInteger}s should return the exact same value that
     * it was given.
     */
    @Test
    public void ifNegativeBigInteger_valid_same() {
        BigInteger value =
                BigInteger.valueOf(randomIntBetween(0, Integer.MAX_VALUE));

        assertSame(value, Exceptions.ifNegative(value, unexpected));
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotPositiveNumber_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotPositive((Double) null, message);
    }

    /**
     * A negative number should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotPositiveNumber_negative_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotPositive(0, message);
    }

    /**
     * A non-negative number should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotPositiveNumber_invalid_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotPositive(randomIntBetween(Integer.MIN_VALUE, -1),
                message);
    }

    /**
     * Positive numbers should not trigger anything.
     */
    @Test
    public void ifNotPositiveNumber_valid() {
        Exceptions.ifNotPositive(1, unexpected);
        Exceptions.ifNotPositive(1L, unexpected);
        Exceptions.ifNotPositive(Float.MIN_VALUE, unexpected);
        Exceptions.ifNotPositive(Double.MIN_VALUE, unexpected);
        Exceptions.ifNotPositive(1234, unexpected);
    }

    /**
     * Positive numbers should return the exact same value that it was given.
     */
    @Test
    public void ifNotPositiveNumber_valid_same() {
        Integer value = randomIntBetween(1, Integer.MAX_VALUE);

        assertSame(value, Exceptions.ifNotPositive(value, unexpected));
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotPositiveBigDecimal_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotPositive((BigDecimal) null, message);
    }

    /**
     * A non-positive {@link BigDecimal} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotPositiveBigDecimal_invalid_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        BigDecimal value =
                BigDecimal.valueOf(randomIntBetween(Integer.MIN_VALUE, 0));

        Exceptions.ifNotPositive(value, message);
    }

    /**
     * {@link BigDecimal#ZERO} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotPositiveBigDecimal_zero_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotPositive(BigDecimal.ZERO, message);
    }

    /**
     * Positive {@link BigDecimal}s should not trigger anything.
     */
    @Test
    public void ifNotPositiveBigDecimal_valid() {
        Exceptions.ifNotPositive(BigDecimal.ONE, unexpected);
        Exceptions.ifNotPositive(BigDecimal.TEN, unexpected);
    }

    /**
     * Positive {@link BigDecimal}s should return the exact same value that it
     * was given.
     */
    @Test
    public void ifNotPositiveBigDecimal_valid_same() {
        BigDecimal value =
                BigDecimal.valueOf(randomIntBetween(1, Integer.MAX_VALUE));

        assertSame(value, Exceptions.ifNotPositive(value, unexpected));
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotPositiveBigInteger_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotPositive((BigInteger) null, message);
    }

    /**
     * A non-positive {@link BigInteger} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotPositiveBigInteger_invalid_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        BigInteger value =
                BigInteger.valueOf(randomIntBetween(Integer.MIN_VALUE, 0));

        Exceptions.ifNotPositive(value, message);
    }

    /**
     * {@link BigInteger#ZERO} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotPositiveBigInteger_zero_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotPositive(BigInteger.ZERO, message);
    }

    /**
     * Positive {@link BigInteger}s should not trigger anything.
     */
    @Test
    public void ifNotPositiveBigInteger_valid() {
        Exceptions.ifNotPositive(BigInteger.ONE, unexpected);
        Exceptions.ifNotPositive(BigInteger.TEN, unexpected);
    }

    /**
     * Positive {@link BigInteger}s should return the exact same value that it
     * was given.
     */
    @Test
    public void ifNotPositiveBigInteger_valid_same() {
        BigInteger value =
                BigInteger.valueOf(randomIntBetween(1, Integer.MAX_VALUE));

        assertSame(value, Exceptions.ifNotPositive(value, unexpected));
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifHasNullArray_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifHasNull((Object[]) null, message);
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifHasNullArray_nullValue_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifHasNull(new Integer[]{null}, message);
    }

    /**
     * An array not containing {@code null} should pass.
     */
    @Test
    public void ifHasNullArray_valid() {
        Exceptions.ifHasNull(new String[]{"non-null"}, unexpected);
    }

    /**
     * An array not containing {@code null} should return the exact same
     * instance with the same generic argument.
     */
    @Test
    public void ifHasNullArray_valid_same() {
        String[] array = new String[] { "value1", "value2" };

        // just guaranteeing that there are no issues with generics
        String[] sameArray = Exceptions.ifHasNull(array, unexpected);

        assertSame(array, sameArray);
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifHasNull_nullCollection_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifHasNull((ArrayList<?>)null, message);
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifHasNull_nullValue_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        ArrayList<Object> list = new ArrayList<>();
        list.add(null);

        Exceptions.ifHasNull(list, message);
    }

    /**
     * A {@code Collection} not containing {@code null} should pass without
     * checking with {@code contains}.
     */
    @Test
    public void ifHasNull_valid_doesNotUseContains() {
        ArrayList<Object> list = new ArrayList<Object>() {
            @Override
            public boolean contains(Object o) {
                // we really don't
                return true;
            }
            @Override
            public int indexOf(Object o) {
                // not really there, but incase someone tried to optimize it
                return 0;
            }
        };
        list.add("non-null");

        Exceptions.ifHasNull(list, unexpected);
    }

    /**
     * A {@code Collection} not containing {@code null} should pass.
     */
    @Test
    public void ifHasNull_valid() {
        ArrayList<Object> list = new ArrayList<>();
        list.add("non-null");

        Exceptions.ifHasNull(list, unexpected);
    }

    /**
     * A {@code Collection} not containing {@code null} should return
     * the exact same instance with the same generic argument.
     */
    @Test
    public void ifHasNull_valid_same() {
        ArrayList<Object> list = new ArrayList<>();
        ArrayList<? extends ArrayList<? extends Integer>> other =
                new ArrayList<>();
        list.add("non-null");

        // just guaranteeing that there are no issues with generics
        ArrayList<Object> verifiedList = Exceptions.ifHasNull(list, unexpected);
        ArrayList<? extends ArrayList<? extends Integer>> verifiedOther =
                Exceptions.ifHasNull(other, unexpected);

        assertSame(list, verifiedList);
        assertSame(other, verifiedOther);
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifContainsNull_nullCollection_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifContainsNull(null, message);
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifContainsNull_nullValue_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        ArrayList<Object> list = new ArrayList<>();
        list.add(null);

        Exceptions.ifContainsNull(list, message);
    }

    /**
     * A {@code Collection} not containing {@code null} should pass.
     */
    @Test
    public void ifContainsNull_valid() {
        ArrayList<Object> list = new ArrayList<>();
        list.add("non-null");

        Exceptions.ifContainsNull(list, unexpected);
    }

    /**
     * A {@code Collection} not containing {@code null} should pass based on the
     * result of {@code contains}.
     */
    @Test
    public void ifContainsNull_valid_usesContains() {
        ArrayList<Object> list = new ArrayList<Object>() {
            @Override
            public boolean contains(Object o) {
                // we really do...
                return false;
            }
        };
        list.add(null);

        Exceptions.ifContainsNull(list, unexpected);
    }

    /**
     * A {@code Collection} not containing {@code null} should return
     * the exact same instance with the same generic argument.
     */
    @Test
    public void ifContainsNull_valid_same() {
        ArrayList<Object> list = new ArrayList<>();
        ArrayList<? extends ArrayList<? extends Integer>> other =
                new ArrayList<>();
        list.add("non-null");

        // just guaranteeing that there are no issues with generics
        ArrayList<Object> verifiedList =
                Exceptions.ifContainsNull(list, unexpected);
        ArrayList<? extends ArrayList<? extends Integer>> verifiedOther =
                Exceptions.ifContainsNull(other, unexpected);

        assertSame(list, verifiedList);
        assertSame(other, verifiedOther);
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotSizeArray_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotSize((Object[]) null, -1, message);
    }

    /**
     * An array whose length is not long enough should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotSizeArray_invalidSize_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Object[] array = { "1", 2, 3d, "4", "5" };

        Exceptions.ifNotSize(array, 10, message);
    }

    /**
     * An array that is long enough should pass.
     */
    @Test
    public void ifNotSizeArray_valid() {
        Object[] array = { "1", 2, 3d, "4", "5" };

        for (int i = 0; i <= array.length; ++i) {
            Exceptions.ifNotSize(array, i, unexpected);
        }
    }

    /**
     * An array that is long enough should return the exact same instance with
     * the same generic argument.
     */
    @Test
    public void ifNotSizeArray_valid_same() {
        int size = randomIntBetween(0, 10);
        Integer[] array = new Integer[size];

        for (int i = 0; i < array.length; ++i) {
            array[i] = i;
        }

        // verify that each size works
        for (int i = 0; i <= size; ++i) {
            // just guaranteeing that there are no issues with generics
            Integer[] sameArray = Exceptions.ifNotSize(array, i, unexpected);

            assertSame(array, sameArray);
            assertArrayEquals(array, sameArray);
        }
    }

    /**
     * {@code null} should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotSizeCollection_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotSize((ArrayList<String>)null, -1, message);
    }

    /**
     * A {@code Collection} whose size is not long enough should trigger an
     * {@link ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotSizeCollection_invalidSize_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        List<?> list = Arrays.asList("1", 2, 3d, "4", "5");

        Exceptions.ifNotSize(list, 10, message);
    }

    /**
     * A {@code Collection} that is big enough should pass.
     */
    @Test
    public void ifNotSizeCollection_valid() {
        List<?> list = Arrays.asList("1", 2, 3d, "4", "5");

        for (int i = 0; i <= list.size(); ++i) {
            Exceptions.ifNotSize(list, i, unexpected);
        }
    }

    /**
     * An array that is long enough should return the exact same instance with
     * the same generic argument.
     */
    @Test
    public void ifNotSizeCollection_valid_same() {
        int size = randomIntBetween(0, 10);
        ArrayList<Integer> list = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            list.add(i);
        }

        // verify that each size works
        for (int length = 0; length <= size; ++length) {
            // just guaranteeing that there are no issues with generics
            ArrayList<Integer> sameList =
                    Exceptions.ifNotSize(list, length, unexpected);

            assertSame(list, sameList);

            // sanity check to ensure nothing was changed
            for (int j = 0; j < size; ++j) {
                assertSame(list.get(j), sameList.get(j));
            }
        }
    }

    /**
     * {@code null} should trigger an an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifTrue_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifTrue(null, message);
    }

    /**
     * {@code true} should trigger an an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifTrue_true_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifTrue(true, message);
    }

    /**
     * {@code false} should pass without throwing an exception.
     */
    @Test
    public void ifTrue_false() {
        Exceptions.ifTrue(false, unexpected);
    }

    /**
     * {@code null} should trigger an an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifFalse_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifFalse(null, message);
    }

    /**
     * {@code false} should trigger an an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifFalse_false_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifFalse(false, message);
    }

    /**
     * {@code true} should pass without throwing an exception.
     */
    @Test
    public void ifFalse_true() {
        Exceptions.ifFalse(true, unexpected);
    }

    /**
     * {@code null} should trigger an an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotInclusive_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotInclusive(null, 1, 2, message);
    }

    /**
     * An out of range value (less than) should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotInclusive_less_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotInclusive(0.99999999, 1d, 2d, message);
    }

    /**
     * An out of range value (greater than) should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotInclusive_greater_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotInclusive(2.00000001, 1d, 2d, message);
    }

    /**
     * Valid inclusive values should not trigger anything.
     */
    @Test
    public void ifNotInclusive_valid() {
        int low = randomIntBetween(Integer.MIN_VALUE, 0);
        int high = randomIntBetween(0, Integer.MAX_VALUE);
        int value = randomIntBetween(low, high);

        BigInteger min = BigInteger.valueOf(low);
        BigInteger max = BigInteger.valueOf(high);
        BigInteger random = BigInteger.valueOf(value);

        Exceptions.ifNotInclusive(value, low, high, unexpected);
        Exceptions.ifNotInclusive(low, low, high, unexpected);
        Exceptions.ifNotInclusive(high, low, high, unexpected);
        Exceptions.ifNotInclusive(min, min, max, unexpected);
        Exceptions.ifNotInclusive(max, min, max, unexpected);
        Exceptions.ifNotInclusive(random, min, max, unexpected);
    }

    /**
     * Valid inclusive values should return the exact same value that it
     * was given.
     */
    @Test
    public void ifNotInclusive_valid_same() {
        int value = randomIntBetween(1, 100);

        assertSame(value, Exceptions.ifNotInclusive(value, 1, 100, unexpected));
        assertSame(value,
                   Exceptions.ifNotInclusive(value, value, 100, unexpected));
        assertSame(value,
                   Exceptions.ifNotInclusive(value, value, value, unexpected));
        assertSame(value,
                   Exceptions.ifNotInclusive(value, 1, value, unexpected));
    }

    /**
     * {@code null} should trigger an an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotExclusive_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotExclusive(null, 1, 2, message);
    }

    /**
     * An out of range value (less than) should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotExclusive_less_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotExclusive(0.99999999, 1d, 2d, message);
    }

    /**
     * An out of range value (equals min) should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotExclusive_equalMin_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotExclusive(1d, 1d, 2d, message);
    }

    /**
     * An out of range value (equals max) should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotExclusive_equalMax_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotExclusive(2d, 1d, 2d, message);
    }

    /**
     * An out of range value (greater than) should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotExclusive_greater_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotExclusive(2.00000001, 1d, 2d, message);
    }

    /**
     * Valid exclusive values should not trigger anything.
     */
    @Test
    public void ifNotExclusive_valid() {
        int low = randomIntBetween(Integer.MIN_VALUE, -1);
        int high = randomIntBetween(1, Integer.MAX_VALUE);
        int value = randomIntBetween(low + 1, high - 1);

        BigInteger min = BigInteger.valueOf(low);
        BigInteger max = BigInteger.valueOf(high);
        BigInteger random = BigInteger.valueOf(value);

        Exceptions.ifNotExclusive(value, low, high, unexpected);
        Exceptions.ifNotExclusive(random, min, max, unexpected);
    }

    /**
     * Valid exclusive values should return the exact same value that it
     * was given.
     */
    @Test
    public void ifNotExclusive_valid_same() {
        int value = randomIntBetween(2, 99);

        assertSame(value, Exceptions.ifNotExclusive(value, 1, 100, unexpected));
    }

    /**
     * {@code null} should trigger an an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotLatitude_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotLatitude(null, message);
    }

    /**
     * An out of range value (less than) should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotLatitude_less_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotLatitude(-90.00000001, message);
    }

    /**
     * An out of range value (greater than) should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotLatitude_greater_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotLatitude(90.00000001, message);
    }

    /**
     * Valid inclusive values should not trigger anything.
     */
    @Test
    public void ifNotLatitude_valid() {
        // [-90, 90)
        Double value = randomIntBetween(-90, 89) + randomDouble();

        Exceptions.ifNotLatitude(value, unexpected);
    }

    /**
     * Valid inclusive values should return the exact same value that it
     * was given.
     */
    @Test
    public void ifNotLatitude_valid_same() {
        Double value = (double)randomIntBetween(-90, 90);

        // exact match
        assertEquals(value, Exceptions.ifNotLatitude(value, unexpected), 0);
    }

    /**
     * {@code null} should trigger an an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotLongitude_null_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotLongitude(null, message);
    }

    /**
     * An out of range value (less than) should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotLongitude_less_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotLongitude(-180.00000001, message);
    }

    /**
     * An out of range value (greater than) should trigger an {@link
     * ElasticsearchIllegalArgumentException}.
     */
    @Test
    public void ifNotLongitude_greater_throwsException() {
        expected.expect(ElasticsearchIllegalArgumentException.class);
        expected.expectMessage(message);

        Exceptions.ifNotLongitude(180.00000001, message);
    }

    /**
     * Valid inclusive values should not trigger anything.
     */
    @Test
    public void ifNotLongitude_valid() {
        // [-180, 180)
        Double value = randomIntBetween(-180, 179) + randomDouble();

        Exceptions.ifNotLongitude(value, unexpected);
    }

    /**
     * Valid inclusive values should return the exact same value that it
     * was given.
     */
    @Test
    public void ifNotLongitude_valid_same() {
        Double value = (double)randomIntBetween(-180, 180);

        // exact match
        assertEquals(value, Exceptions.ifNotLongitude(value, unexpected), 0);
    }

    /**
     * The constructor is not being used, but utility code should generally be
     * safe from issues and covered by tests.
     */
    @Test
    public void constructor_isPrivate_withoutIssues()
            throws NoSuchMethodException,
                   IllegalAccessException,
                   InvocationTargetException,
                   InstantiationException {
        Constructor<Exceptions> ctor =
                Exceptions.class.getDeclaredConstructor();

        assertFalse(ctor.isAccessible());

        try {
            ctor.setAccessible(true);

            // should not be doing anything
            ctor.newInstance();
        }
        finally {
            ctor.setAccessible(false);
        }
    }
}
