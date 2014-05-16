package org.elasticsearch;

import org.elasticsearch.common.Strings;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;

/**
 * {@code Exceptions} exists to provide a mechanism for quickly testing incoming
 * parameters to any function or constructor in order to guarantee their code
 * contract. This also reduces the likelihood of a typo (e.g., {@code !=}
 * becoming {@code ==}, or similar) in such contracts by giving readability.
 * <p />
 * This replaces long-winded code such as
 * <p />
 * <pre>{@code
 * if (value1 == null) {
 *   throw new ElasticsearchIllegalArgumentException("value1 cannot be null");
 * }
 * if (Strings.isNullOrEmpty(value2)) {
 *   throw new ElasticsearchIllegalArgumentException("value2 cannot be empty");
 * }
 *
 * // required
 * this.value1 = value1;
 * this.value2 = value2;
 * // optional
 * this.value3 = value3;
 * }</pre>
 * <p />
 * with shorter code such as
 * <p />
 * <pre>{@code
 * // required
 * this.value1 = Exceptions.ifNull(value1, "value1 cannot be null");
 * this.value2 = Exceptions.ifEmpty(value2, "value2 cannot be empty");
 * // optional
 * this.value3 = value3;
 * }</pre>
 * <p />
 * By using a static import of {@code Exceptions}, the code can be shortened
 * further.
 */
public final class Exceptions {

    /**
     * {@link Exceptions} is a utility meant to assist with easily throwing
     * exceptions that fail code contracts. As such, it is not meant to be
     * instantiable (and therefore itself needed to be checked).
     */
    private Exceptions() {
        // uninstantiable singleton
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null}.
     * @param value The value to test
     * @param message The message to use with the exception
     * @return Always {@code value}. Never {@code null}.
     * @throws ElasticsearchIllegalArgumentException if {@code value} is {@code
     *                                               null}
     */
    public static <T> T ifNull(T value, String message) {
        if (value == null) {
            throwIllegal(message);
        }

        return value;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null} or {@link String#isEmpty() empty}.
     * <p />
     * Note: Being non-empty only indicates that it is at least one character,
     * which could itself be some form of whitespace. Consider using {@link
     * #ifNotHasText} instead.
     * @param value The value to test
     * @param message The message to use with the exception
     * @return Always {@code value}. Never {@code null} or {@link
     *         String#isEmpty() empty}.
     * @throws ElasticsearchIllegalArgumentException if {@code value} is {@code
     *                                               null} or {@link
     *                                               String#isEmpty() empty}
     */
    public static String ifEmpty(String value, String message) {
        if (value == null || value.isEmpty()) {
            throwIllegal(message);
        }

        return value;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null}, {@link String#isEmpty() empty} or made up
     * entirely of whitespace (blank).
     * @param value The value to test
     * @param message The message to use with the exception
     * @return Always {@code value}. Never {@code null}, {@link String#isEmpty()
     *         empty} or made up entirely of whitespace (blank).
     * @throws ElasticsearchIllegalArgumentException if {@code value} does not
     *                                               contain any text ({@code
     *                                               null} or all whitespace)
     */
    public static String ifNotHasText(String value, String message) {
        if ( ! Strings.hasText(value)) {
            throwIllegal(message, value);
        }

        return value;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null} or negative.
     * @param value The value to test
     * @param message The message to use with the exception
     * @param <T> Any implementation of {@link Number}.
     * @return Always {@code value}. Never {@code null} and always greater than
     *         or equal to zero.
     * @throws ElasticsearchIllegalArgumentException if {@code value} is {@code
     *                                               null} or &lt; 0
     */
    public static <T extends Number> T ifNegative(T value, String message) {
        if (value == null || value.doubleValue() < 0) {
            throwIllegal(message, value);
        }

        return value;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null} or negative.
     * @param value The value to test
     * @param message The message to use with the exception
     * @return Always {@code value}. Never {@code null} and always greater than
     *         or equal to {@link BigDecimal#ZERO}.
     * @throws ElasticsearchIllegalArgumentException if {@code value} is {@code
     *                                               null} or &lt; 0
     */
    public static BigDecimal ifNegative(BigDecimal value, String message) {
        if (value == null || value.compareTo(BigDecimal.ZERO) < 0) {
            throwIllegal(message, value);
        }

        return value;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null} or negative.
     * @param value The value to test
     * @param message The message to use with the exception
     * @return Always {@code value}. Never {@code null} and always greater than
     *         or equal to {@link BigInteger#ZERO}.
     * @throws ElasticsearchIllegalArgumentException if {@code value} is {@code
     *                                               null} or &lt; 0
     */
    public static BigInteger ifNegative(BigInteger value, String message) {
        if (value == null || value.compareTo(BigInteger.ZERO) < 0) {
            throwIllegal(message, value);
        }

        return value;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null} or not greater than zero.
     * @param value The value to test
     * @param message The message to use with the exception
     * @return Always {@code value}. Never {@code null} and always greater than
     *         zero.
     * @throws ElasticsearchIllegalArgumentException if {@code value} is {@code
     *                                               null} or &lt;= 0
     */
    public static <T extends Number> T ifNotPositive(T value, String message) {
        if (value == null || value.doubleValue() <= 0) {
            throwIllegal(message, value);
        }

        return value;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null} or not greater than {@link BigDecimal#ZERO}.
     * @param value The value to test
     * @param message The message to use with the exception
     * @return Always {@code value}. Never {@code null} and always greater than
     *         {@link BigDecimal#ZERO}.
     * @throws ElasticsearchIllegalArgumentException if {@code value} is {@code
     *                                               null} or &lt;= 0
     */
    public static BigDecimal ifNotPositive(BigDecimal value, String message) {
        if (value == null || value.compareTo(BigDecimal.ZERO) <= 0) {
            throwIllegal(message, value);
        }

        return value;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null} or not greater than {@link BigInteger#ZERO}.
     * @param value The value to test
     * @param message The message to use with the exception
     * @return Always {@code value}. Never {@code null} and always greater than
     *         {@link BigInteger#ZERO}.
     * @throws ElasticsearchIllegalArgumentException if {@code value} is {@code
     *                                               null} or &lt;= 0
     */
    public static BigInteger ifNotPositive(BigInteger value, String message) {
        if (value == null || value.compareTo(BigInteger.ZERO) <= 0) {
            throwIllegal(message, value);
        }

        return value;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * array} is {@code null} or {@code array.length} {@code null}.
     * @param array The array to test
     * @param message The message to use with the exception
     * @return Always {@code collection}. Never {@code null}. Never contains
     *         {@code null}.
     * @throws ElasticsearchIllegalArgumentException if {@code array} is {@code
     *                                               null} or contains {@code
     *                                               null}
     */
    public static <T> T[] ifHasNull(T[] array, String message) {
        if (array == null) {
            throwIllegal(message);
        }
        else {
            for (Object value : array) {
                if (value == null) {
                    throwIllegal(message);
                }
            }
        }

        return array;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * collection} is {@code null} or {@link Collection#contains} {@code null}.
     * <p />
     * Note: This will loop across the {@code collection} in lieu of using
     * {@link Collection#contains} to avoid the allowed {@link
     * NullPointerException} if the {@code collection} does not allow
     * {@code null}s.
     * <p />
     * With that in mind, if you are always using those types of collections,
     * then you should only use {@link #ifNull(Object, String)}.
     * @param collection The collection to test
     * @param message The message to use with the exception
     * @return Always {@code collection}. Never {@code null}. Never contains
     *         {@code null}.
     * @throws ElasticsearchIllegalArgumentException if {@code collection} is
     *                                               {@code null} or contains
     *                                               {@code null}
     */
    public static <V, T extends Collection<V>> T ifHasNull(T collection,
                                                           String message) {
        if (collection == null) {
            throwIllegal(message);
        }
        else {
            for (Object value : collection) {
                if (value == null) {
                    throwIllegal(message);
                }
            }
        }

        return collection;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * collection} is {@code null} or {@link Collection#contains} {@code null}.
     * <p />
     * Note: If the {@code collection}, {@code T}, does not support {@code null}
     * values within it (e.g., Guava-based collections), then it is free to
     * raise a {@link NullPointerException}. To avoid this behavior, you can
     * use the safer, but likely slower {@link #ifHasNull(Collection, String)}.
     * <p />
     * With that in mind, if you are always using those types of collections,
     * then you should only use {@link #ifNull(Object, String)}.
     * @param collection The collection to test
     * @param message The message to use with the exception
     * @return Always {@code collection}. Never {@code null}. Never contains
     *         {@code null}.
     * @throws ElasticsearchIllegalArgumentException if {@code collection} is
     *                                               {@code null} or contains
     *                                               {@code null}
     */
    public static <V, T extends Collection<V>> T ifContainsNull(T collection,
                                                                String message) {
        if (collection == null || collection.contains(null)) {
            throwIllegal(message);
        }

        return collection;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null} or {@code array.length} is less than {@code
     * size}.
     * <p />
     * Note: This will not work with primitive arrays.
     * @param array The array to test
     * @param size The minimum size of the collection
     * @param message The message to use with the exception
     * @return Always {@code array}. Never {@code null}. Never smaller than the
     *         {@code size}.
     * @throws ElasticsearchIllegalArgumentException if {@code array} is {@code
     *                                               null} or if {@code
     *                                               array.length} is &lt;
     *                                               {@code size}
     */
    public static <T> T[] ifNotSize(T[] array, int size, String message) {
        if (array == null) {
            throwIllegal(message);
        }
        else if (array.length < size) {
            throwIllegal(message, array.length);
        }

        return array;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null} or {@link Collection#size} is less than {@code
     * size}.
     * @param collection The collection to test
     * @param size The minimum size of the collection
     * @param message The message to use with the exception
     * @return Always {@code collection}. Never {@code null}. Never smaller than
     *         the {@code size}.
     * @throws ElasticsearchIllegalArgumentException if {@code collection} is
     *                                               {@code null} or if its
     *                                               {@link Collection#size()}
     *                                               is &lt; {@code size}
     */
    public static <V, T extends Collection<V>> T ifNotSize(T collection,
                                                           int size,
                                                           String message) {
        if (collection == null) {
            throwIllegal(message);
        }
        else if (collection.size() < size) {
            throwIllegal(message, collection.size());
        }

        return collection;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * condition} is {@code null} or {@code true}.
     * @param condition The value to test
     * @param message The message to use with the exception
     * @throws ElasticsearchIllegalArgumentException if {@code condition} is
     *                                               {@code null} or {@code
     *                                               true}
     */
    public static void ifTrue(Boolean condition, String message) {
        if (condition == null || condition) {
            throwIllegal(message);
        }
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * condition} is {@code null} or {@code false}.
     * @param condition The value to test
     * @param message The message to use with the exception
     * @throws ElasticsearchIllegalArgumentException if {@code condition} is
     *                                               {@code null} or {@code
     *                                               false}
     */
    public static void ifFalse(Boolean condition, String message) {
        if (condition == null || ! condition) {
            throwIllegal(message);
        }
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null} and within the range of [{@code minimum}, {@code
     * maximum}].
     * @param value The value to test
     * @param minimum The minimum inclusive value
     * @param maximum The maximum inclusive value
     * @param message The message to use with the exception
     * @param <T> Expected to be a {@link Number}, but any {@link Comparable} is
     *            allowed.
     * @return Always {@code value}. Never {@code null}. Always within the
     *         range of [{@code minimum}, {@code maximum}].
     * @throws ElasticsearchIllegalArgumentException if {@code value} is
     *                                               {@code null}, &lt; {@code
     *                                               minimum} or &gt; {@code
     *                                               maximum}
     * @throws NullPointerException if {@code minimum} or {@code maximum} is
     *                              {@code null}
     */
    public static <T extends Comparable<T>> T ifNotInclusive(T value,
                                                             T minimum,
                                                             T maximum,
                                                             String message) {
        if (value == null ||
            value.compareTo(minimum) < 0 || value.compareTo(maximum) > 0) {
            throwIllegal(message, value);
        }

        return value;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null} and within the range of ({@code minimum}, {@code
     * maximum}).
     * @param value The value to test
     * @param minimum The minimum exclusive value
     * @param maximum The maximum exclusive value
     * @param message The message to use with the exception
     * @param <T> Expected to be a {@link Number}, but any {@link Comparable} is
     *            allowed.
     * @return Always {@code value}. Never {@code null}. Always within the
     *         range of ({@code minimum}, {@code maximum}).
     * @throws ElasticsearchIllegalArgumentException if {@code value} is
     *                                               {@code null}, &lt;= {@code
     *                                               minimum} or &gt;= {@code
     *                                               maximum}
     */
    public static <T extends Comparable<T>> T ifNotExclusive(T value,
                                                             T minimum,
                                                             T maximum,
                                                             String message) {
        if (value == null ||
            value.compareTo(minimum) <= 0 || value.compareTo(maximum) >= 0) {
            throwIllegal(message, value);
        }

        return value;
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * value} is {@code null} and within the range of [-90, 90].
     * @param latitude The latitude to test
     * @param message The message to use with the exception
     * @return Always {@code latitude}. Never {@code null}. Always within the
     *         range of [-90, 90].
     * @throws ElasticsearchIllegalArgumentException if {@code value} is
     *                                               {@code null}, &lt; -90 or
     *                                               &gt; 90
     */
    public static double ifNotLatitude(Double latitude, String message) {
        return ifNotInclusive(latitude, -90d, 90d, message);
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} if the {@code
     * longitude} is {@code null} and within the range of [-90, 90].
     * @param longitude The longitude to test
     * @param message The message to use with the exception
     * @return Always {@code longitude}. Never {@code null}. Always within the
     *         range of [-180, 180].
     * @throws ElasticsearchIllegalArgumentException if {@code value} is
     *                                               {@code null}, &lt; -180 or
     *                                               &gt; 180
     */
    public static double ifNotLongitude(Double longitude, String message) {
        return ifNotInclusive(longitude, -180d, 180d, message);
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException}.
     * @param message The message to use with the exception
     * @throws ElasticsearchIllegalArgumentException always.
     */
    private static void throwIllegal(String message) {
        throw new ElasticsearchIllegalArgumentException(message);
    }

    /**
     * Throw an {@link ElasticsearchIllegalArgumentException} that contains the
     * {@code value} appended to the {@code message}.
     * @param message The message to use with the exception
     * @param value The value that caused the exception
     * @throws ElasticsearchIllegalArgumentException always.
     */
    private static void throwIllegal(String message, Object value) {
        String valueMessage = String.format(" (Value was [%s])", value);
        throwIllegal(message + valueMessage);
    }
}
