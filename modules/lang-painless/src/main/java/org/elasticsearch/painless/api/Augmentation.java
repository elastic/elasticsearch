/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.api;

import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.painless.PainlessScript;

import java.nio.charset.StandardCharsets;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.ObjLongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/** Additional methods added to classes. These must be static methods with receiver as first argument */
public class Augmentation {

    // static methods only!
    private Augmentation() {}

    /** Exposes List.size() as getLength(), so that .length shortcut works on lists */
    public static <T> int getLength(List<T> receiver) {
        return receiver.size();
    }

    /** Exposes Matcher.group(String) as namedGroup(String), so it doesn't conflict with group(int) */
    public static String namedGroup(Matcher receiver, String name) {
        return receiver.group(name);
    }

    /**
     * Cancellation-aware wrapper around {@link Iterable#forEach}.  Polls
     * {@link PainlessScript#_pollCancellation()} once per element so a long iteration honours search
     * timeouts.  Fast path delegates straight to the JDK when the script has no cancellation check
     * installed.
     */
    public static <T> void forEach(PainlessScript script, Iterable<T> receiver, Consumer<T> consumer) {
        receiver.forEach(wrap(script, consumer));
    }

    // some groovy methods on iterable
    // see http://docs.groovy-lang.org/latest/html/groovy-jdk/java/lang/Iterable.html

    /**
     * Cancellation-aware {@code any}.  Returns true if {@code predicate} matches at least one element;
     * short-circuits on the first match.  When iteration continues past a non-matching element the
     * augmentation calls {@link PainlessScript#_pollCancellation()} so a worst-case (no-match) scan of
     * a large collection still honours search timeouts.  Delegates to {@link #any(Iterable, Predicate)}
     * when the script has no cancellation check installed.
     */
    public static <T> boolean any(PainlessScript script, Iterable<T> receiver, Predicate<T> predicate) {
        if (script._getCancellationCheck() == null) {
            return any(receiver, predicate);
        }
        for (T t : receiver) {
            if (predicate.test(t)) {
                return true;
            }
            script._pollCancellation();
        }
        return false;
    }

    /** Iterates over the contents of an iterable, and checks whether a predicate is valid for at least one element. */
    public static <T> boolean any(Iterable<T> receiver, Predicate<T> predicate) {
        for (T t : receiver) {
            if (predicate.test(t)) {
                return true;
            }
        }
        return false;
    }

    /** Converts this Iterable to a Collection. Returns the original Iterable if it is already a Collection. */
    public static <T> Collection<T> asCollection(Iterable<T> receiver) {
        if (receiver instanceof Collection) {
            return (Collection<T>) receiver;
        }
        List<T> list = new ArrayList<>();
        for (T t : receiver) {
            list.add(t);
        }
        return list;
    }

    /** Converts this Iterable to a List. Returns the original Iterable if it is already a List. */
    public static <T> List<T> asList(Iterable<T> receiver) {
        if (receiver instanceof List) {
            return (List<T>) receiver;
        }
        List<T> list = new ArrayList<>();
        for (T t : receiver) {
            list.add(t);
        }
        return list;
    }

    /**
     * Cancellation-aware {@code count}.  Counts the elements matching {@code predicate} by scanning the whole
     * iterable; the wrapped predicate polls {@link PainlessScript#_pollCancellation()} once per element so
     * counting a large collection honours search timeouts, and is the unwrapped predicate (JDK fast path) when
     * the script has no cancellation check installed.
     */
    public static <T> int count(PainlessScript script, Iterable<T> receiver, Predicate<T> predicate) {
        return count(receiver, wrap(script, predicate));
    }

    /** Counts the number of occurrences which satisfy the given predicate from inside this Iterable. */
    public static <T> int count(Iterable<T> receiver, Predicate<T> predicate) {
        int count = 0;
        for (T t : receiver) {
            if (predicate.test(t)) {
                count++;
            }
        }
        return count;
    }

    // instead of covariant overrides for every possibility, we just return receiver as 'def' for now
    // that way if someone chains the calls, everything works.

    /**
     * Cancellation-aware {@code each} augmentation resolved by the lookup
     * builder in script contexts whose base class supports cancellation.  Calls
     * {@link PainlessScript#_pollCancellation()} once per element so the script's shared poll
     * counter advances and the search timeout can interrupt a long iteration even when the consumer
     * body is trivial or non-Painless.  When {@code _getCancellationCheck()} returns null (script
     * context didn't wire a cancel runnable) the fast path delegates straight to
     * {@link Iterable#forEach} with zero per-iteration overhead.
     * <p>
     * The script receiver is placed <em>before</em> the augmentation's receiver so that
     * {@link org.elasticsearch.painless.FunctionRef#withSyntheticScriptCapture} (which prepends
     * the script class at factoryMethodType position 0) maps directly to a method reference's
     * leading capture without any position arithmetic.
     */
    public static <T> Object each(PainlessScript script, Iterable<T> receiver, Consumer<T> consumer) {
        if (script._getCancellationCheck() == null) {
            receiver.forEach(consumer);
            return receiver;
        }
        // Poll the script's shared persistent counter once per element rather than keeping our own,
        // so this iteration's polling decrements the same $cancelPoll that the consumer body and the
        // rest of the script decrement — keeping the cadence amortised and respecting downstream work.
        for (T t : receiver) {
            consumer.accept(t);
            script._pollCancellation();
        }
        return receiver;
    }

    /**
     * Cancellation-aware {@code eachWithIndex} augmentation.  Visits each element of {@code receiver}
     * paired with a zero-based index, polling {@link PainlessScript#_pollCancellation()} once per
     * element so the script's shared poll counter advances and a search timeout can interrupt iteration
     * even when the consumer body is trivial.  Delegates to {@link #eachWithIndex(Iterable, ObjIntConsumer)}
     * when the script has no cancellation check installed.
     */
    public static <T> Object eachWithIndex(PainlessScript script, Iterable<T> receiver, ObjIntConsumer<T> consumer) {
        if (script._getCancellationCheck() == null) {
            return eachWithIndex(receiver, consumer);
        }
        int count = 0;
        for (T t : receiver) {
            consumer.accept(t, count++);
            script._pollCancellation();
        }
        return receiver;
    }

    /**
     * Iterates through an iterable type, passing each item and the item's index
     * (a counter starting at zero) to the given consumer.
     */
    public static <T> Object eachWithIndex(Iterable<T> receiver, ObjIntConsumer<T> consumer) {
        int count = 0;
        for (T t : receiver) {
            consumer.accept(t, count++);
        }
        return receiver;
    }

    /**
     * Cancellation-aware {@code every}.  Returns true only if {@code predicate} matches every element;
     * short-circuits on the first non-match.  Polls {@link PainlessScript#_pollCancellation()} once per
     * matching element so a worst-case (all-match) scan honours search timeouts.  Delegates to
     * {@link #every(Iterable, Predicate)} when the script has no cancellation check installed.
     */
    public static <T> boolean every(PainlessScript script, Iterable<T> receiver, Predicate<T> predicate) {
        if (script._getCancellationCheck() == null) {
            return every(receiver, predicate);
        }
        for (T t : receiver) {
            if (predicate.test(t) == false) {
                return false;
            }
            script._pollCancellation();
        }
        return true;
    }

    /**
     * Used to determine if the given predicate is valid (i.e. returns true for all items in this iterable).
     */
    public static <T> boolean every(Iterable<T> receiver, Predicate<T> predicate) {
        for (T t : receiver) {
            if (predicate.test(t) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Cancellation-aware {@code findResults}.  Applies {@code filter} to every element and collects
     * non-null results into a new list, polling {@link PainlessScript#_pollCancellation()} once per
     * element so the full scan honours search timeouts.  Delegates to
     * {@link #findResults(Iterable, Function)} when the script has no cancellation check installed.
     */
    public static <T, U> List<U> findResults(PainlessScript script, Iterable<T> receiver, Function<T, U> filter) {
        if (script._getCancellationCheck() == null) {
            return findResults(receiver, filter);
        }
        List<U> list = new ArrayList<>();
        for (T t : receiver) {
            U result = filter.apply(t);
            if (result != null) {
                list.add(result);
            }
            script._pollCancellation();
        }
        return list;
    }

    /**
     * Iterates through the Iterable transforming items using the supplied function and
     * collecting any non-null results.
     */
    public static <T, U> List<U> findResults(Iterable<T> receiver, Function<T, U> filter) {
        List<U> list = new ArrayList<>();
        for (T t : receiver) {
            U result = filter.apply(t);
            if (result != null) {
                list.add(result);
            }
        }
        return list;
    }

    /**
     * Cancellation-aware {@code groupBy}.  Builds a {@link LinkedHashMap} grouping each element by the
     * value returned from {@code mapper}, polling {@link PainlessScript#_pollCancellation()} once per
     * element so the full scan honours search timeouts.  Delegates to
     * {@link #groupBy(Iterable, Function)} when the script has no cancellation check installed.
     */
    public static <T, U> Map<U, List<T>> groupBy(PainlessScript script, Iterable<T> receiver, Function<T, U> mapper) {
        if (script._getCancellationCheck() == null) {
            return groupBy(receiver, mapper);
        }
        Map<U, List<T>> map = new LinkedHashMap<>();
        for (T t : receiver) {
            map.computeIfAbsent(mapper.apply(t), k -> new ArrayList<>()).add(t);
            script._pollCancellation();
        }
        return map;
    }

    /**
     * Sorts all Iterable members into groups determined by the supplied mapping function.
     */
    public static <T, U> Map<U, List<T>> groupBy(Iterable<T> receiver, Function<T, U> mapper) {
        Map<U, List<T>> map = new LinkedHashMap<>();
        for (T t : receiver) {
            map.computeIfAbsent(mapper.apply(t), k -> new ArrayList<>()).add(t);
        }
        return map;
    }

    /**
     * Concatenates the toString() representation of each item in this Iterable,
     * with the given String as a separator between each item.
     */
    public static <T> String join(Iterable<T> receiver, String separator) {
        StringBuilder sb = new StringBuilder();
        boolean firstToken = true;
        for (T t : receiver) {
            if (firstToken) {
                firstToken = false;
            } else {
                sb.append(separator);
            }
            sb.append(t);
        }
        return sb.toString();
    }

    /**
     * Sums the result of an Iterable
     */
    public static <T extends Number> double sum(Iterable<T> receiver) {
        double sum = 0;
        for (T t : receiver) {
            sum += t.doubleValue();
        }
        return sum;
    }

    /**
     * Cancellation-aware {@code sum(ToDoubleFunction)}.  Applies {@code function} to each element and
     * accumulates the result, polling {@link PainlessScript#_pollCancellation()} once per element so
     * the full scan honours search timeouts.  Delegates to {@link #sum(Iterable, ToDoubleFunction)} when
     * the script has no cancellation check installed.
     */
    public static <T> double sum(PainlessScript script, Iterable<T> receiver, ToDoubleFunction<T> function) {
        if (script._getCancellationCheck() == null) {
            return sum(receiver, function);
        }
        double sum = 0;
        for (T t : receiver) {
            sum += function.applyAsDouble(t);
            script._pollCancellation();
        }
        return sum;
    }

    /**
     * Sums the result of applying a function to each item of an Iterable.
     */
    public static <T> double sum(Iterable<T> receiver, ToDoubleFunction<T> function) {
        double sum = 0;
        for (T t : receiver) {
            sum += function.applyAsDouble(t);
        }
        return sum;
    }

    /**
     * Cancellation-aware wrapper around {@link Collection#removeIf}.  Iterates explicitly via the
     * receiver's iterator so each {@link Predicate#test} invocation can be followed by a
     * {@link PainlessScript#_pollCancellation()} call.  Fast path delegates straight to the JDK when
     * the script has no cancellation check installed (preserving optimisations on collections that
     * override removeIf, e.g. {@code ArrayList}).
     */
    public static <T> boolean removeIf(PainlessScript script, Collection<T> receiver, Predicate<T> predicate) {
        if (script._getCancellationCheck() == null) {
            return receiver.removeIf(predicate);
        }
        boolean removed = false;
        Iterator<T> iter = receiver.iterator();
        while (iter.hasNext()) {
            if (predicate.test(iter.next())) {
                iter.remove();
                removed = true;
            }
            script._pollCancellation();
        }
        return removed;
    }

    // some groovy methods on collection
    // see http://docs.groovy-lang.org/latest/html/groovy-jdk/java/util/Collection.html

    /**
     * Cancellation-aware {@code collect(Function)}.  Maps every element of {@code receiver} via
     * {@code function} into a new list, polling {@link PainlessScript#_pollCancellation()} once per
     * element.  Delegates to {@link #collect(Collection, Function)} when the script has no
     * cancellation check installed.
     */
    public static <T, U> List<U> collect(PainlessScript script, Collection<T> receiver, Function<T, U> function) {
        if (script._getCancellationCheck() == null) {
            return collect(receiver, function);
        }
        List<U> list = new ArrayList<>();
        for (T t : receiver) {
            list.add(function.apply(t));
            script._pollCancellation();
        }
        return list;
    }

    /**
     * Iterates through this collection transforming each entry into a new value using
     * the function, returning a list of transformed values.
     */
    public static <T, U> List<U> collect(Collection<T> receiver, Function<T, U> function) {
        List<U> list = new ArrayList<>();
        for (T t : receiver) {
            list.add(function.apply(t));
        }
        return list;
    }

    /**
     * Cancellation-aware {@code collect(Collection, Function)}.  Maps every element of {@code receiver}
     * via {@code function} and appends each result to {@code collection}, polling
     * {@link PainlessScript#_pollCancellation()} once per element.  Delegates to
     * {@link #collect(Collection, Collection, Function)} when the script has no cancellation check
     * installed.
     */
    public static <T, U> Object collect(PainlessScript script, Collection<T> receiver, Collection<U> collection, Function<T, U> function) {
        if (script._getCancellationCheck() == null) {
            return collect(receiver, collection, function);
        }
        for (T t : receiver) {
            collection.add(function.apply(t));
            script._pollCancellation();
        }
        return collection;
    }

    /**
     * Iterates through this collection transforming each entry into a new value using
     * the function, adding the values to the specified collection.
     */
    public static <T, U> Object collect(Collection<T> receiver, Collection<U> collection, Function<T, U> function) {
        for (T t : receiver) {
            collection.add(function.apply(t));
        }
        return collection;
    }

    /**
     * Cancellation-aware {@code find}.  Returns the first element matching {@code predicate}, or
     * {@code null}; short-circuits on the first match.  Polls
     * {@link PainlessScript#_pollCancellation()} once per non-matching element so a worst-case
     * (no-match) scan honours search timeouts.  Delegates to {@link #find(Collection, Predicate)}
     * when the script has no cancellation check installed.
     */
    public static <T> T find(PainlessScript script, Collection<T> receiver, Predicate<T> predicate) {
        if (script._getCancellationCheck() == null) {
            return find(receiver, predicate);
        }
        for (T t : receiver) {
            if (predicate.test(t)) {
                return t;
            }
            script._pollCancellation();
        }
        return null;
    }

    /**
     * Finds the first value matching the predicate, or returns null.
     */
    public static <T> T find(Collection<T> receiver, Predicate<T> predicate) {
        for (T t : receiver) {
            if (predicate.test(t)) {
                return t;
            }
        }
        return null;
    }

    /**
     * Cancellation-aware {@code findAll}.  Collects every element matching {@code predicate}, polling
     * {@link PainlessScript#_pollCancellation()} once per element so the full scan honours search
     * timeouts.  Delegates to {@link #findAll(Collection, Predicate)} when the script has no
     * cancellation check installed.
     */
    public static <T> List<T> findAll(PainlessScript script, Collection<T> receiver, Predicate<T> predicate) {
        if (script._getCancellationCheck() == null) {
            return findAll(receiver, predicate);
        }
        List<T> list = new ArrayList<>();
        for (T t : receiver) {
            if (predicate.test(t)) {
                list.add(t);
            }
            script._pollCancellation();
        }
        return list;
    }

    /**
     * Finds all values matching the predicate, returns as a list
     */
    public static <T> List<T> findAll(Collection<T> receiver, Predicate<T> predicate) {
        List<T> list = new ArrayList<>();
        for (T t : receiver) {
            if (predicate.test(t)) {
                list.add(t);
            }
        }
        return list;
    }

    /**
     * Cancellation-aware {@code findResult(Function)}.  Delegates to the three-arg script-aware
     * overload with a {@code null} default.
     */
    public static <T, U> Object findResult(PainlessScript script, Collection<T> receiver, Function<T, U> function) {
        return findResult(script, receiver, null, function);
    }

    /**
     * Iterates through the collection calling the given function for each item
     * but stopping once the first non-null result is found and returning that result.
     * If all results are null, null is returned.
     */
    public static <T, U> Object findResult(Collection<T> receiver, Function<T, U> function) {
        return findResult(receiver, null, function);
    }

    /**
     * Cancellation-aware {@code findResult(Object, Function)}.  Returns the first non-null result of
     * applying {@code function} to an element, or {@code defaultResult} when no element produces a
     * non-null result.  Polls {@link PainlessScript#_pollCancellation()} after each null-yielding
     * element so a worst-case (all-null) scan honours search timeouts.  Delegates to
     * {@link #findResult(Collection, Object, Function)} when the script has no cancellation check
     * installed.
     */
    public static <T, U> Object findResult(PainlessScript script, Collection<T> receiver, Object defaultResult, Function<T, U> function) {
        if (script._getCancellationCheck() == null) {
            return findResult(receiver, defaultResult, function);
        }
        for (T t : receiver) {
            U value = function.apply(t);
            if (value != null) {
                return value;
            }
            script._pollCancellation();
        }
        return defaultResult;
    }

    /**
     * Iterates through the collection calling the given function for each item
     * but stopping once the first non-null result is found and returning that result.
     * If all results are null, defaultResult is returned.
     */
    public static <T, U> Object findResult(Collection<T> receiver, Object defaultResult, Function<T, U> function) {
        for (T t : receiver) {
            U value = function.apply(t);
            if (value != null) {
                return value;
            }
        }
        return defaultResult;
    }

    /**
     * Cancellation-aware {@code split}.  Partitions {@code receiver} into matched and unmatched lists,
     * polling {@link PainlessScript#_pollCancellation()} once per element so the full scan honours
     * search timeouts.  Delegates to {@link #split(Collection, Predicate)} when the script has no
     * cancellation check installed.
     */
    public static <T> List<List<T>> split(PainlessScript script, Collection<T> receiver, Predicate<T> predicate) {
        if (script._getCancellationCheck() == null) {
            return split(receiver, predicate);
        }
        List<T> matched = new ArrayList<>();
        List<T> unmatched = new ArrayList<>();
        List<List<T>> result = new ArrayList<>(2);
        result.add(matched);
        result.add(unmatched);
        for (T t : receiver) {
            if (predicate.test(t)) {
                matched.add(t);
            } else {
                unmatched.add(t);
            }
            script._pollCancellation();
        }
        return result;
    }

    /**
     * Splits all items into two collections based on the predicate.
     * The first list contains all items which match the closure expression. The second list all those that don't.
     */
    public static <T> List<List<T>> split(Collection<T> receiver, Predicate<T> predicate) {
        List<T> matched = new ArrayList<>();
        List<T> unmatched = new ArrayList<>();
        List<List<T>> result = new ArrayList<>(2);
        result.add(matched);
        result.add(unmatched);
        for (T t : receiver) {
            if (predicate.test(t)) {
                matched.add(t);
            } else {
                unmatched.add(t);
            }
        }
        return result;
    }

    /**
     * Cancellation-aware wrapper around {@link Map#forEach}.  Iterates the entry set and polls
     * {@link PainlessScript#_pollCancellation()} once per entry.  Fast path delegates straight to the
     * JDK when the script has no cancellation check installed.
     */
    public static <K, V> void forEach(PainlessScript script, Map<K, V> receiver, BiConsumer<K, V> consumer) {
        receiver.forEach(wrap(script, consumer));
    }

    /**
     * Cancellation-aware wrapper around {@link Map#replaceAll}.  Walks the entry set, computes each
     * new value via {@code function}, and writes it back through {@link Map.Entry#setValue}, polling
     * {@link PainlessScript#_pollCancellation()} once per entry.  Fast path delegates straight to the
     * JDK when the script has no cancellation check installed.
     */
    public static <K, V> void replaceAll(PainlessScript script, Map<K, V> receiver, BiFunction<K, V, V> function) {
        if (script._getCancellationCheck() == null) {
            receiver.replaceAll(function);
            return;
        }
        for (Map.Entry<K, V> entry : receiver.entrySet()) {
            entry.setValue(function.apply(entry.getKey(), entry.getValue()));
            script._pollCancellation();
        }
    }

    // some groovy methods on map
    // see http://docs.groovy-lang.org/latest/html/groovy-jdk/java/util/Map.html

    /**
     * Cancellation-aware {@code collect(BiFunction)} on Map.  Maps every entry via {@code function}
     * into a new list, polling {@link PainlessScript#_pollCancellation()} once per entry.  Delegates
     * to {@link #collect(Map, BiFunction)} when the script has no cancellation check installed.
     */
    public static <K, V, T> List<T> collect(PainlessScript script, Map<K, V> receiver, BiFunction<K, V, T> function) {
        if (script._getCancellationCheck() == null) {
            return collect(receiver, function);
        }
        List<T> list = new ArrayList<>();
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            list.add(function.apply(kvPair.getKey(), kvPair.getValue()));
            script._pollCancellation();
        }
        return list;
    }

    /**
     * Iterates through this map transforming each entry into a new value using
     * the function, returning a list of transformed values.
     */
    public static <K, V, T> List<T> collect(Map<K, V> receiver, BiFunction<K, V, T> function) {
        List<T> list = new ArrayList<>();
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            list.add(function.apply(kvPair.getKey(), kvPair.getValue()));
        }
        return list;
    }

    /**
     * Cancellation-aware {@code collect(Collection, BiFunction)} on Map.  Maps every entry via
     * {@code function} and appends the result to {@code collection}, polling
     * {@link PainlessScript#_pollCancellation()} once per entry.  Delegates to
     * {@link #collect(Map, Collection, BiFunction)} when the script has no cancellation check installed.
     */
    public static <K, V, T> Object collect(
        PainlessScript script,
        Map<K, V> receiver,
        Collection<T> collection,
        BiFunction<K, V, T> function
    ) {
        if (script._getCancellationCheck() == null) {
            return collect(receiver, collection, function);
        }
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            collection.add(function.apply(kvPair.getKey(), kvPair.getValue()));
            script._pollCancellation();
        }
        return collection;
    }

    /**
     * Iterates through this map transforming each entry into a new value using
     * the function, adding the values to the specified collection.
     */
    public static <K, V, T> Object collect(Map<K, V> receiver, Collection<T> collection, BiFunction<K, V, T> function) {
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            collection.add(function.apply(kvPair.getKey(), kvPair.getValue()));
        }
        return collection;
    }

    /**
     * Cancellation-aware {@code count} on Map.  Counts entries matching {@code predicate}, polling
     * {@link PainlessScript#_pollCancellation()} once per entry.  Delegates to
     * {@link #count(Map, BiPredicate)} when the script has no cancellation check installed.
     */
    public static <K, V> int count(PainlessScript script, Map<K, V> receiver, BiPredicate<K, V> predicate) {
        if (script._getCancellationCheck() == null) {
            return count(receiver, predicate);
        }
        int count = 0;
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            if (predicate.test(kvPair.getKey(), kvPair.getValue())) {
                count++;
            }
            script._pollCancellation();
        }
        return count;
    }

    /** Counts the number of occurrences which satisfy the given predicate from inside this Map */
    public static <K, V> int count(Map<K, V> receiver, BiPredicate<K, V> predicate) {
        int count = 0;
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            if (predicate.test(kvPair.getKey(), kvPair.getValue())) {
                count++;
            }
        }
        return count;
    }

    /**
     * Cancellation-aware {@code each} on Map.  Visits every entry, polling
     * {@link PainlessScript#_pollCancellation()} once per entry.  Fast path delegates straight to
     * {@link Map#forEach} when the script has no cancellation check installed.
     */
    public static <K, V> Object each(PainlessScript script, Map<K, V> receiver, BiConsumer<K, V> consumer) {
        if (script._getCancellationCheck() == null) {
            receiver.forEach(consumer);
            return receiver;
        }
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            consumer.accept(kvPair.getKey(), kvPair.getValue());
            script._pollCancellation();
        }
        return receiver;
    }

    /**
     * Cancellation-aware {@code every} on Map.  Returns true only if {@code predicate} matches every
     * entry; short-circuits on the first non-match.  Polls {@link PainlessScript#_pollCancellation()}
     * once per matching entry.  Delegates to {@link #every(Map, BiPredicate)} when the script has no
     * cancellation check installed.
     */
    public static <K, V> boolean every(PainlessScript script, Map<K, V> receiver, BiPredicate<K, V> predicate) {
        if (script._getCancellationCheck() == null) {
            return every(receiver, predicate);
        }
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            if (predicate.test(kvPair.getKey(), kvPair.getValue()) == false) {
                return false;
            }
            script._pollCancellation();
        }
        return true;
    }

    /**
     * Used to determine if the given predicate is valid (i.e. returns true for all items in this map).
     */
    public static <K, V> boolean every(Map<K, V> receiver, BiPredicate<K, V> predicate) {
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            if (predicate.test(kvPair.getKey(), kvPair.getValue()) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Cancellation-aware {@code find} on Map.  Returns the first entry matching {@code predicate}, or
     * {@code null}; short-circuits on the first match.  Polls {@link PainlessScript#_pollCancellation()}
     * once per non-matching entry.  Delegates to {@link #find(Map, BiPredicate)} when the script has
     * no cancellation check installed.
     */
    public static <K, V> Map.Entry<K, V> find(PainlessScript script, Map<K, V> receiver, BiPredicate<K, V> predicate) {
        if (script._getCancellationCheck() == null) {
            return find(receiver, predicate);
        }
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            if (predicate.test(kvPair.getKey(), kvPair.getValue())) {
                return kvPair;
            }
            script._pollCancellation();
        }
        return null;
    }

    /**
     * Finds the first entry matching the predicate, or returns null.
     */
    public static <K, V> Map.Entry<K, V> find(Map<K, V> receiver, BiPredicate<K, V> predicate) {
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            if (predicate.test(kvPair.getKey(), kvPair.getValue())) {
                return kvPair;
            }
        }
        return null;
    }

    /**
     * Cancellation-aware {@code findAll} on Map.  Collects entries matching {@code predicate} into a
     * new {@link LinkedHashMap} (or {@link TreeMap} when the receiver is one), polling
     * {@link PainlessScript#_pollCancellation()} once per entry.  Delegates to
     * {@link #findAll(Map, BiPredicate)} when the script has no cancellation check installed.
     */
    public static <K, V> Map<K, V> findAll(PainlessScript script, Map<K, V> receiver, BiPredicate<K, V> predicate) {
        if (script._getCancellationCheck() == null) {
            return findAll(receiver, predicate);
        }
        final Map<K, V> map;
        if (receiver instanceof TreeMap) {
            map = new TreeMap<>();
        } else {
            map = new LinkedHashMap<>();
        }
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            if (predicate.test(kvPair.getKey(), kvPair.getValue())) {
                map.put(kvPair.getKey(), kvPair.getValue());
            }
            script._pollCancellation();
        }
        return map;
    }

    /**
     * Finds all values matching the predicate, returns as a map.
     */
    public static <K, V> Map<K, V> findAll(Map<K, V> receiver, BiPredicate<K, V> predicate) {
        // try to preserve some properties of the receiver (see the groovy javadocs)
        final Map<K, V> map;
        if (receiver instanceof TreeMap) {
            map = new TreeMap<>();
        } else {
            map = new LinkedHashMap<>();
        }
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            if (predicate.test(kvPair.getKey(), kvPair.getValue())) {
                map.put(kvPair.getKey(), kvPair.getValue());
            }
        }
        return map;
    }

    /**
     * Cancellation-aware {@code findResult(BiFunction)} on Map.  Delegates to the four-arg script-aware
     * overload with a {@code null} default.
     */
    public static <K, V, T> Object findResult(PainlessScript script, Map<K, V> receiver, BiFunction<K, V, T> function) {
        return findResult(script, receiver, null, function);
    }

    /**
     * Iterates through the map calling the given function for each item
     * but stopping once the first non-null result is found and returning that result.
     * If all results are null, null is returned.
     */
    public static <K, V, T> Object findResult(Map<K, V> receiver, BiFunction<K, V, T> function) {
        return findResult(receiver, null, function);
    }

    /**
     * Cancellation-aware {@code findResult(Object, BiFunction)} on Map.  Returns the first non-null
     * result of applying {@code function} to an entry, or {@code defaultResult} when none does.  Polls
     * {@link PainlessScript#_pollCancellation()} after each null-yielding entry.  Delegates to
     * {@link #findResult(Map, Object, BiFunction)} when the script has no cancellation check installed.
     */
    public static <K, V, T> Object findResult(
        PainlessScript script,
        Map<K, V> receiver,
        Object defaultResult,
        BiFunction<K, V, T> function
    ) {
        if (script._getCancellationCheck() == null) {
            return findResult(receiver, defaultResult, function);
        }
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            T value = function.apply(kvPair.getKey(), kvPair.getValue());
            if (value != null) {
                return value;
            }
            script._pollCancellation();
        }
        return defaultResult;
    }

    /**
     * Iterates through the map calling the given function for each item
     * but stopping once the first non-null result is found and returning that result.
     * If all results are null, defaultResult is returned.
     */
    public static <K, V, T> Object findResult(Map<K, V> receiver, Object defaultResult, BiFunction<K, V, T> function) {
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            T value = function.apply(kvPair.getKey(), kvPair.getValue());
            if (value != null) {
                return value;
            }
        }
        return defaultResult;
    }

    /**
     * Cancellation-aware {@code findResults} on Map.  Applies {@code filter} to every entry and
     * collects non-null results into a new list, polling {@link PainlessScript#_pollCancellation()}
     * once per entry.  Delegates to {@link #findResults(Map, BiFunction)} when the script has no
     * cancellation check installed.
     */
    public static <K, V, T> List<T> findResults(PainlessScript script, Map<K, V> receiver, BiFunction<K, V, T> filter) {
        if (script._getCancellationCheck() == null) {
            return findResults(receiver, filter);
        }
        List<T> list = new ArrayList<>();
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            T result = filter.apply(kvPair.getKey(), kvPair.getValue());
            if (result != null) {
                list.add(result);
            }
            script._pollCancellation();
        }
        return list;
    }

    /**
     * Iterates through the map transforming items using the supplied function and
     * collecting any non-null results.
     */
    public static <K, V, T> List<T> findResults(Map<K, V> receiver, BiFunction<K, V, T> filter) {
        List<T> list = new ArrayList<>();
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            T result = filter.apply(kvPair.getKey(), kvPair.getValue());
            if (result != null) {
                list.add(result);
            }
        }
        return list;
    }

    /**
     * Cancellation-aware {@code groupBy} on Map.  Groups entries by the value returned from
     * {@code mapper}, polling {@link PainlessScript#_pollCancellation()} once per entry.  Delegates to
     * {@link #groupBy(Map, BiFunction)} when the script has no cancellation check installed.
     */
    public static <K, V, T> Map<T, Map<K, V>> groupBy(PainlessScript script, Map<K, V> receiver, BiFunction<K, V, T> mapper) {
        if (script._getCancellationCheck() == null) {
            return groupBy(receiver, mapper);
        }
        Map<T, Map<K, V>> map = new LinkedHashMap<>();
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            T mapped = mapper.apply(kvPair.getKey(), kvPair.getValue());
            map.computeIfAbsent(mapped, k -> receiver instanceof TreeMap ? new TreeMap<K, V>() : new LinkedHashMap<K, V>())
                .put(kvPair.getKey(), kvPair.getValue());
            script._pollCancellation();
        }
        return map;
    }

    /**
     * Sorts all Map members into groups determined by the supplied mapping function.
     */
    public static <K, V, T> Map<T, Map<K, V>> groupBy(Map<K, V> receiver, BiFunction<K, V, T> mapper) {
        Map<T, Map<K, V>> map = new LinkedHashMap<>();
        for (Map.Entry<K, V> kvPair : receiver.entrySet()) {
            T mapped = mapper.apply(kvPair.getKey(), kvPair.getValue());
            // try to preserve some properties of the receiver (see the groovy javadocs)
            map.computeIfAbsent(mapped, k -> receiver instanceof TreeMap ? new TreeMap<K, V>() : new LinkedHashMap<K, V>())
                .put(kvPair.getKey(), kvPair.getValue());
        }
        return map;
    }

    // native wrappers for cancellation-aware iteration

    /**
     * Cancellation-aware wrapper around {@link List#replaceAll}.  Uses a {@link ListIterator} so each
     * {@link UnaryOperator#apply} call can be followed by a {@link PainlessScript#_pollCancellation()}
     * call.  Fast path delegates straight to the JDK when the script has no cancellation check
     * installed (preserving optimisations on lists that override replaceAll, e.g. {@code ArrayList}).
     */
    public static <T> void replaceAll(PainlessScript script, List<T> receiver, UnaryOperator<T> operator) {
        if (script._getCancellationCheck() == null) {
            receiver.replaceAll(operator);
            return;
        }
        ListIterator<T> iter = receiver.listIterator();
        while (iter.hasNext()) {
            iter.set(operator.apply(iter.next()));
            script._pollCancellation();
        }
    }

    /**
     * Cancellation-aware wrapper around {@link Iterator#forEachRemaining}.  Walks the receiver via
     * {@link Iterator#hasNext}/{@link Iterator#next} and polls {@link PainlessScript#_pollCancellation()}
     * once per element.  Fast path delegates straight to the JDK when the script has no cancellation
     * check installed.
     */
    public static <T> void forEachRemaining(PainlessScript script, Iterator<T> receiver, Consumer<T> consumer) {
        receiver.forEachRemaining(wrap(script, consumer));
    }

    /**
     * Cancellation-aware wrapper around {@link Spliterator#forEachRemaining}.  Drives the receiver
     * via {@link Spliterator#tryAdvance} so the augmentation can poll
     * {@link PainlessScript#_pollCancellation()} between each element rather than letting the
     * spliterator's bulk traversal run uninterrupted.  Fast path delegates straight to the JDK when
     * the script has no cancellation check installed.
     */
    public static <T> void forEachRemaining(PainlessScript script, Spliterator<T> receiver, Consumer<T> consumer) {
        receiver.forEachRemaining(wrap(script, consumer));
    }

    // Wrap a user-supplied functional interface so it polls _pollCancellation() once per element after delegating to
    // the wrapped action, or return it unchanged when no cancellation check is installed so the caller still takes the
    // JDK fast path. There is one overload per functional-interface type used by the native and stream-terminal
    // wrappers; result-returning interfaces capture the result, poll, then return it. The primitive overloads are kept
    // distinct (rather than a single generic Consumer wrapper) so they never box their elements.

    @SuppressWarnings("overloads")
    private static <T> Consumer<T> wrap(PainlessScript script, Consumer<T> action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return t -> {
            action.accept(t);
            script._pollCancellation();
        };
    }

    @SuppressWarnings("overloads")
    private static <A, B> BiConsumer<A, B> wrap(PainlessScript script, BiConsumer<A, B> action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return (a, b) -> {
            action.accept(a, b);
            script._pollCancellation();
        };
    }

    @SuppressWarnings("overloads")
    private static <T> Predicate<T> wrap(PainlessScript script, Predicate<T> action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return t -> {
            boolean result = action.test(t);
            script._pollCancellation();
            return result;
        };
    }

    @SuppressWarnings("overloads")
    private static <T> BinaryOperator<T> wrap(PainlessScript script, BinaryOperator<T> action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return (a, b) -> {
            T result = action.apply(a, b);
            script._pollCancellation();
            return result;
        };
    }

    @SuppressWarnings("overloads")
    private static <A, B, R> BiFunction<A, B, R> wrap(PainlessScript script, BiFunction<A, B, R> action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return (a, b) -> {
            R result = action.apply(a, b);
            script._pollCancellation();
            return result;
        };
    }

    @SuppressWarnings("overloads")
    private static IntConsumer wrap(PainlessScript script, IntConsumer action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return i -> {
            action.accept(i);
            script._pollCancellation();
        };
    }

    @SuppressWarnings("overloads")
    private static LongConsumer wrap(PainlessScript script, LongConsumer action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return l -> {
            action.accept(l);
            script._pollCancellation();
        };
    }

    @SuppressWarnings("overloads")
    private static DoubleConsumer wrap(PainlessScript script, DoubleConsumer action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return d -> {
            action.accept(d);
            script._pollCancellation();
        };
    }

    @SuppressWarnings("overloads")
    private static IntPredicate wrap(PainlessScript script, IntPredicate action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return i -> {
            boolean result = action.test(i);
            script._pollCancellation();
            return result;
        };
    }

    @SuppressWarnings("overloads")
    private static LongPredicate wrap(PainlessScript script, LongPredicate action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return l -> {
            boolean result = action.test(l);
            script._pollCancellation();
            return result;
        };
    }

    @SuppressWarnings("overloads")
    private static DoublePredicate wrap(PainlessScript script, DoublePredicate action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return d -> {
            boolean result = action.test(d);
            script._pollCancellation();
            return result;
        };
    }

    @SuppressWarnings("overloads")
    private static IntBinaryOperator wrap(PainlessScript script, IntBinaryOperator action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return (a, b) -> {
            int result = action.applyAsInt(a, b);
            script._pollCancellation();
            return result;
        };
    }

    @SuppressWarnings("overloads")
    private static LongBinaryOperator wrap(PainlessScript script, LongBinaryOperator action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return (a, b) -> {
            long result = action.applyAsLong(a, b);
            script._pollCancellation();
            return result;
        };
    }

    @SuppressWarnings("overloads")
    private static DoubleBinaryOperator wrap(PainlessScript script, DoubleBinaryOperator action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return (a, b) -> {
            double result = action.applyAsDouble(a, b);
            script._pollCancellation();
            return result;
        };
    }

    @SuppressWarnings("overloads")
    private static <R> ObjIntConsumer<R> wrap(PainlessScript script, ObjIntConsumer<R> action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return (r, i) -> {
            action.accept(r, i);
            script._pollCancellation();
        };
    }

    @SuppressWarnings("overloads")
    private static <R> ObjLongConsumer<R> wrap(PainlessScript script, ObjLongConsumer<R> action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return (r, l) -> {
            action.accept(r, l);
            script._pollCancellation();
        };
    }

    @SuppressWarnings("overloads")
    private static <R> ObjDoubleConsumer<R> wrap(PainlessScript script, ObjDoubleConsumer<R> action) {
        if (script._getCancellationCheck() == null) {
            return action;
        }
        return (r, d) -> {
            action.accept(r, d);
            script._pollCancellation();
        };
    }

    /**
     * Cancellation-aware wrapper around {@link PrimitiveIterator.OfInt#forEachRemaining(IntConsumer)}.  Polls
     * {@link PainlessScript#_pollCancellation()} once per element via the wrapped {@link IntConsumer}; takes the
     * JDK fast path unchanged when no cancellation check is installed.
     */
    public static void forEachRemaining(PainlessScript script, PrimitiveIterator.OfInt receiver, IntConsumer action) {
        receiver.forEachRemaining(wrap(script, action));
    }

    /**
     * Cancellation-aware wrapper around {@link PrimitiveIterator.OfLong#forEachRemaining(LongConsumer)}.  Polls
     * {@link PainlessScript#_pollCancellation()} once per element via the wrapped {@link LongConsumer}; takes the
     * JDK fast path unchanged when no cancellation check is installed.
     */
    public static void forEachRemaining(PainlessScript script, PrimitiveIterator.OfLong receiver, LongConsumer action) {
        receiver.forEachRemaining(wrap(script, action));
    }

    /**
     * Cancellation-aware wrapper around {@link PrimitiveIterator.OfDouble#forEachRemaining(DoubleConsumer)}.  Polls
     * {@link PainlessScript#_pollCancellation()} once per element via the wrapped {@link DoubleConsumer}; takes the
     * JDK fast path unchanged when no cancellation check is installed.
     */
    public static void forEachRemaining(PainlessScript script, PrimitiveIterator.OfDouble receiver, DoubleConsumer action) {
        receiver.forEachRemaining(wrap(script, action));
    }

    /**
     * Cancellation-aware wrapper around {@link Spliterator.OfInt#forEachRemaining(IntConsumer)}.  Polls
     * {@link PainlessScript#_pollCancellation()} once per element via the wrapped {@link IntConsumer}; takes the
     * JDK fast path unchanged when no cancellation check is installed.
     */
    public static void forEachRemaining(PainlessScript script, Spliterator.OfInt receiver, IntConsumer action) {
        receiver.forEachRemaining(wrap(script, action));
    }

    /**
     * Cancellation-aware wrapper around {@link Spliterator.OfLong#forEachRemaining(LongConsumer)}.  Polls
     * {@link PainlessScript#_pollCancellation()} once per element via the wrapped {@link LongConsumer}; takes the
     * JDK fast path unchanged when no cancellation check is installed.
     */
    public static void forEachRemaining(PainlessScript script, Spliterator.OfLong receiver, LongConsumer action) {
        receiver.forEachRemaining(wrap(script, action));
    }

    /**
     * Cancellation-aware wrapper around {@link Spliterator.OfDouble#forEachRemaining(DoubleConsumer)}.  Polls
     * {@link PainlessScript#_pollCancellation()} once per element via the wrapped {@link DoubleConsumer}; takes the
     * JDK fast path unchanged when no cancellation check is installed.
     */
    public static void forEachRemaining(PainlessScript script, Spliterator.OfDouble receiver, DoubleConsumer action) {
        receiver.forEachRemaining(wrap(script, action));
    }

    // stream terminal wrappers for cancellation-aware iteration
    //
    // Each wrapper takes the user's functional-interface argument and wraps it in a polling lambda so
    // the JDK's terminal-op iteration drives the pipeline while {@link PainlessScript#_pollCancellation()}
    // fires once per element. Intermediate operations (filter/map/flatMap/peek) are not wrapped:
    // polling at the terminal op covers the whole pipeline because terminal ops pull elements through
    // all upstream stages. Fast path delegates straight to the JDK call when no cancellation check is
    // installed. Painless does not expose Stream#parallel(), so the wrapping lambdas only have to be
    // correct for sequential streams.

    /** Cancellation-aware wrapper around {@link Stream#forEach}. */
    public static <T> void forEach(PainlessScript script, Stream<T> receiver, Consumer<T> consumer) {
        receiver.forEach(wrap(script, consumer));
    }

    /** Cancellation-aware wrapper around {@link Stream#forEachOrdered}. */
    public static <T> void forEachOrdered(PainlessScript script, Stream<T> receiver, Consumer<T> consumer) {
        receiver.forEachOrdered(wrap(script, consumer));
    }

    /** Cancellation-aware wrapper around {@link Stream#allMatch}. */
    public static <T> boolean allMatch(PainlessScript script, Stream<T> receiver, Predicate<T> predicate) {
        return receiver.allMatch(wrap(script, predicate));
    }

    /** Cancellation-aware wrapper around {@link Stream#anyMatch}. */
    public static <T> boolean anyMatch(PainlessScript script, Stream<T> receiver, Predicate<T> predicate) {
        return receiver.anyMatch(wrap(script, predicate));
    }

    /** Cancellation-aware wrapper around {@link Stream#noneMatch}. */
    public static <T> boolean noneMatch(PainlessScript script, Stream<T> receiver, Predicate<T> predicate) {
        return receiver.noneMatch(wrap(script, predicate));
    }

    /** Cancellation-aware wrapper around {@link Stream#reduce(BinaryOperator)}. */
    public static <T> Optional<T> reduce(PainlessScript script, Stream<T> receiver, BinaryOperator<T> op) {
        return receiver.reduce(wrap(script, op));
    }

    /** Cancellation-aware wrapper around {@link Stream#reduce(Object, BinaryOperator)}. */
    public static <T> T reduce(PainlessScript script, Stream<T> receiver, T identity, BinaryOperator<T> op) {
        return receiver.reduce(identity, wrap(script, op));
    }

    /** Cancellation-aware wrapper around {@link Stream#reduce(Object, BiFunction, BinaryOperator)}. */
    public static <T, U> U reduce(
        PainlessScript script,
        Stream<T> receiver,
        U identity,
        BiFunction<U, ? super T, U> accumulator,
        BinaryOperator<U> combiner
    ) {
        return receiver.reduce(identity, wrap(script, accumulator), combiner);
    }

    /** Cancellation-aware wrapper around {@link Stream#collect(Supplier, BiConsumer, BiConsumer)}. */
    public static <T, R> R collect(
        PainlessScript script,
        Stream<T> receiver,
        Supplier<R> supplier,
        BiConsumer<R, ? super T> accumulator,
        BiConsumer<R, R> combiner
    ) {
        return receiver.collect(supplier, wrap(script, accumulator), combiner);
    }

    /**
     * Cancellation-aware wrapper around {@link Stream#collect(Collector)}.  Unlike the other stream
     * terminals there is no per-element argument to wrap directly — the accumulator is bundled inside
     * the {@link Collector} — so this decomposes the collector and rebuilds an equivalent one whose
     * accumulator also polls {@link PainlessScript#_pollCancellation()} once per element.  Painless does
     * not expose {@link Stream#parallel()}, so the stream is always sequential: the accumulator runs once
     * per element and the combiner is never invoked.  The original characteristics (including
     * {@code IDENTITY_FINISH}) and finisher are preserved unchanged, so the result is identical to the JDK.
     * Fast path delegates straight to the JDK when the script has no cancellation check installed.
     */
    public static <T, A, R> R collect(PainlessScript script, Stream<T> receiver, Collector<? super T, A, R> collector) {
        if (script._getCancellationCheck() == null) {
            return receiver.collect(collector);
        }
        BiConsumer<A, ? super T> accumulator = collector.accumulator();
        Collector<T, A, R> polling = Collector.of(collector.supplier(), (a, t) -> {
            accumulator.accept(a, t);
            script._pollCancellation();
        }, collector.combiner(), collector.finisher(), collector.characteristics().toArray(new Collector.Characteristics[0]));
        return receiver.collect(polling);
    }

    /** Cancellation-aware wrapper around {@link IntStream#forEach}. */
    public static void forEach(PainlessScript script, IntStream receiver, IntConsumer consumer) {
        receiver.forEach(wrap(script, consumer));
    }

    /** Cancellation-aware wrapper around {@link IntStream#forEachOrdered}. */
    public static void forEachOrdered(PainlessScript script, IntStream receiver, IntConsumer consumer) {
        receiver.forEachOrdered(wrap(script, consumer));
    }

    /** Cancellation-aware wrapper around {@link IntStream#allMatch}. */
    public static boolean allMatch(PainlessScript script, IntStream receiver, IntPredicate predicate) {
        return receiver.allMatch(wrap(script, predicate));
    }

    /** Cancellation-aware wrapper around {@link IntStream#anyMatch}. */
    public static boolean anyMatch(PainlessScript script, IntStream receiver, IntPredicate predicate) {
        return receiver.anyMatch(wrap(script, predicate));
    }

    /** Cancellation-aware wrapper around {@link IntStream#noneMatch}. */
    public static boolean noneMatch(PainlessScript script, IntStream receiver, IntPredicate predicate) {
        return receiver.noneMatch(wrap(script, predicate));
    }

    /** Cancellation-aware wrapper around {@link IntStream#reduce(IntBinaryOperator)}. */
    public static OptionalInt reduce(PainlessScript script, IntStream receiver, IntBinaryOperator op) {
        return receiver.reduce(wrap(script, op));
    }

    /** Cancellation-aware wrapper around {@link IntStream#reduce(int, IntBinaryOperator)}. */
    public static int reduce(PainlessScript script, IntStream receiver, int identity, IntBinaryOperator op) {
        return receiver.reduce(identity, wrap(script, op));
    }

    /** Cancellation-aware wrapper around {@link IntStream#collect(Supplier, ObjIntConsumer, BiConsumer)}. */
    public static <R> R collect(
        PainlessScript script,
        IntStream receiver,
        Supplier<R> supplier,
        ObjIntConsumer<R> accumulator,
        BiConsumer<R, R> combiner
    ) {
        return receiver.collect(supplier, wrap(script, accumulator), combiner);
    }

    /** Cancellation-aware wrapper around {@link LongStream#forEach}. */
    public static void forEach(PainlessScript script, LongStream receiver, LongConsumer consumer) {
        receiver.forEach(wrap(script, consumer));
    }

    /** Cancellation-aware wrapper around {@link LongStream#forEachOrdered}. */
    public static void forEachOrdered(PainlessScript script, LongStream receiver, LongConsumer consumer) {
        receiver.forEachOrdered(wrap(script, consumer));
    }

    /** Cancellation-aware wrapper around {@link LongStream#allMatch}. */
    public static boolean allMatch(PainlessScript script, LongStream receiver, LongPredicate predicate) {
        return receiver.allMatch(wrap(script, predicate));
    }

    /** Cancellation-aware wrapper around {@link LongStream#anyMatch}. */
    public static boolean anyMatch(PainlessScript script, LongStream receiver, LongPredicate predicate) {
        return receiver.anyMatch(wrap(script, predicate));
    }

    /** Cancellation-aware wrapper around {@link LongStream#noneMatch}. */
    public static boolean noneMatch(PainlessScript script, LongStream receiver, LongPredicate predicate) {
        return receiver.noneMatch(wrap(script, predicate));
    }

    /** Cancellation-aware wrapper around {@link LongStream#reduce(LongBinaryOperator)}. */
    public static OptionalLong reduce(PainlessScript script, LongStream receiver, LongBinaryOperator op) {
        return receiver.reduce(wrap(script, op));
    }

    /** Cancellation-aware wrapper around {@link LongStream#reduce(long, LongBinaryOperator)}. */
    public static long reduce(PainlessScript script, LongStream receiver, long identity, LongBinaryOperator op) {
        return receiver.reduce(identity, wrap(script, op));
    }

    /** Cancellation-aware wrapper around {@link LongStream#collect(Supplier, ObjLongConsumer, BiConsumer)}. */
    public static <R> R collect(
        PainlessScript script,
        LongStream receiver,
        Supplier<R> supplier,
        ObjLongConsumer<R> accumulator,
        BiConsumer<R, R> combiner
    ) {
        return receiver.collect(supplier, wrap(script, accumulator), combiner);
    }

    /** Cancellation-aware wrapper around {@link DoubleStream#forEach}. */
    public static void forEach(PainlessScript script, DoubleStream receiver, DoubleConsumer consumer) {
        receiver.forEach(wrap(script, consumer));
    }

    /** Cancellation-aware wrapper around {@link DoubleStream#forEachOrdered}. */
    public static void forEachOrdered(PainlessScript script, DoubleStream receiver, DoubleConsumer consumer) {
        receiver.forEachOrdered(wrap(script, consumer));
    }

    /** Cancellation-aware wrapper around {@link DoubleStream#allMatch}. */
    public static boolean allMatch(PainlessScript script, DoubleStream receiver, DoublePredicate predicate) {
        return receiver.allMatch(wrap(script, predicate));
    }

    /** Cancellation-aware wrapper around {@link DoubleStream#anyMatch}. */
    public static boolean anyMatch(PainlessScript script, DoubleStream receiver, DoublePredicate predicate) {
        return receiver.anyMatch(wrap(script, predicate));
    }

    /** Cancellation-aware wrapper around {@link DoubleStream#noneMatch}. */
    public static boolean noneMatch(PainlessScript script, DoubleStream receiver, DoublePredicate predicate) {
        return receiver.noneMatch(wrap(script, predicate));
    }

    /** Cancellation-aware wrapper around {@link DoubleStream#reduce(DoubleBinaryOperator)}. */
    public static OptionalDouble reduce(PainlessScript script, DoubleStream receiver, DoubleBinaryOperator op) {
        return receiver.reduce(wrap(script, op));
    }

    /** Cancellation-aware wrapper around {@link DoubleStream#reduce(double, DoubleBinaryOperator)}. */
    public static double reduce(PainlessScript script, DoubleStream receiver, double identity, DoubleBinaryOperator op) {
        return receiver.reduce(identity, wrap(script, op));
    }

    /** Cancellation-aware wrapper around {@link DoubleStream#collect(Supplier, ObjDoubleConsumer, BiConsumer)}. */
    public static <R> R collect(
        PainlessScript script,
        DoubleStream receiver,
        Supplier<R> supplier,
        ObjDoubleConsumer<R> accumulator,
        BiConsumer<R, R> combiner
    ) {
        return receiver.collect(supplier, wrap(script, accumulator), combiner);
    }

    // CharSequence augmentation
    /**
     * Cancellation-aware {@code replaceAll(Pattern, Function)}.  Similar to {@link Matcher#replaceAll(String)} but lets the script
     * customise the replacement per match.  Two protections layered on top of the JDK call:
     * <ul>
     *   <li>The receiver is wrapped in {@link LimitedCharSequence} so the regex engine's
     *       {@code charAt} reads are bounded by the {@code script.painless.regex.limit-factor}
     *       setting (closing the gap that previously left {@code String.replaceAll(Pattern, Function)}
     *       unprotected against catastrophic backtracking).</li>
     *   <li>When the script has a cancellation check installed, polls
     *       {@link PainlessScript#_pollCancellation()} once per match so a many-match sweep honours
     *       the search timeout.</li>
     * </ul>
     */
    public static String replaceAll(
        PainlessScript script,
        CharSequence receiver,
        int limitFactor,
        Pattern pattern,
        Function<Matcher, String> replacementBuilder
    ) {
        CharSequence input = limitFactor == UNLIMITED_PATTERN_FACTOR ? receiver : new LimitedCharSequence(receiver, pattern, limitFactor);
        Matcher m = pattern.matcher(input);
        if (false == m.find()) {
            // CharSequence's toString is *supposed* to always return the characters in the sequence as a String
            return receiver.toString();
        }
        StringBuilder result = new StringBuilder(initialBufferForReplaceWith(receiver));
        if (script._getCancellationCheck() == null) {
            do {
                m.appendReplacement(result, Matcher.quoteReplacement(replacementBuilder.apply(m)));
            } while (m.find());
        } else {
            do {
                m.appendReplacement(result, Matcher.quoteReplacement(replacementBuilder.apply(m)));
                script._pollCancellation();
            } while (m.find());
        }
        m.appendTail(result);
        return result.toString();
    }

    /**
     * Regex-limited {@code replaceFirst(Pattern, Function)}.  Similar to {@link Matcher#replaceFirst(String)} but lets the script
     * customise the replacement based on the match.  The receiver is wrapped in {@link LimitedCharSequence} so the regex engine's
     * {@code charAt} reads are bounded by the {@code script.painless.regex.limit-factor} setting (same protection as
     * {@link #replaceAll(PainlessScript, CharSequence, int, Pattern, Function)}).  Per-match polling is not added because at most
     * one match is performed.
     */
    public static String replaceFirst(
        CharSequence receiver,
        int limitFactor,
        Pattern pattern,
        Function<Matcher, String> replacementBuilder
    ) {
        CharSequence input = limitFactor == UNLIMITED_PATTERN_FACTOR ? receiver : new LimitedCharSequence(receiver, pattern, limitFactor);
        Matcher m = pattern.matcher(input);
        if (false == m.find()) {
            // CharSequence's toString is *supposed* to always return the characters in the sequence as a String
            return receiver.toString();
        }
        StringBuilder result = new StringBuilder(initialBufferForReplaceWith(receiver));
        m.appendReplacement(result, Matcher.quoteReplacement(replacementBuilder.apply(m)));
        m.appendTail(result);
        return result.toString();
    }

    /**
     * Characters copied between cancellation polls inside the script-aware {@code replace}.  Kept large so each copy is an
     * efficient bulk operation; since the script's persistent poll counter itself only fires every 1000 decrements, an
     * actual cancellation check lands roughly every 64,000,000 characters.
     */
    private static final int REPLACE_POLL_INTERVAL = 64000;

    /**
     * Cancellation-aware augmentation for {@link String#replace(CharSequence, CharSequence)}.  The JDK call is O(length)
     * but invisible to the cancellation budget, so a growing-string loop (e.g. {@code s = s.replace('A', 'AB')}) can burn
     * unbounded CPU.  Reimplements the literal replace as a scan that polls {@link PainlessScript#_pollCancellation()} as
     * it works, so a deadline interrupts it mid-call.  Delegates to the JDK method when no cancel check is set.
     */
    public static String replace(PainlessScript script, String receiver, CharSequence target, CharSequence replacement) {
        if (script._getCancellationCheck() == null) {
            return receiver.replace(target, replacement);
        }

        String targetString = target.toString();
        String replacementString = replacement.toString();
        StringBuilder result = new StringBuilder();

        if (targetString.isEmpty()) {
            // Match the JDK: replacement before the first char, between every pair, and after the last.
            result.append(replacementString);
            for (int index = 0; index < receiver.length(); index++) {
                result.append(receiver.charAt(index)).append(replacementString);
                if (index % REPLACE_POLL_INTERVAL == 0) {
                    script._pollCancellation();
                }
            }
            return result.toString();
        }

        int from = 0;
        for (int match = receiver.indexOf(targetString); match >= 0; match = receiver.indexOf(targetString, from)) {
            appendChunked(script, result, receiver, from, match);
            result.append(replacementString);
            script._pollCancellation();
            from = match + targetString.length();
        }
        appendChunked(script, result, receiver, from, receiver.length());
        return result.toString();
    }

    /** Appends {@code sequence[start, end)} in chunks, polling after each so a single large copy cannot outrun a deadline. */
    private static void appendChunked(PainlessScript script, StringBuilder result, CharSequence sequence, int start, int end) {
        int index = start;
        while (end - index > REPLACE_POLL_INTERVAL) {
            result.append(sequence, index, index + REPLACE_POLL_INTERVAL);
            script._pollCancellation();
            index += REPLACE_POLL_INTERVAL;
        }
        result.append(sequence, index, end);
    }

    /**
     * Cancellation-aware {@link String#indexOf(String)}.  Reimplements the search with an explicit O(n*m) outer-position scan so the
     * augmentation can poll {@link PainlessScript#_pollCancellation()} between candidate starting positions.  This guards against a
     * malicious haystack/needle pair (lots of false-positive prefix matches) running uninterrupted past the search timeout.
     * Delegates to the JDK call when no cancellation check is installed.
     */
    public static int indexOf(PainlessScript script, String receiver, String search) {
        if (script._getCancellationCheck() == null) {
            return receiver.indexOf(search);
        }
        return indexOfWithPolling(script, receiver, search, 0);
    }

    /**
     * Cancellation-aware {@link String#indexOf(String, int)}.  Same as {@link #indexOf(PainlessScript, String, String)} but starts
     * the search at {@code fromIndex}.  Matches the JDK's clamping behaviour for {@code fromIndex} outside the receiver's bounds.
     */
    public static int indexOf(PainlessScript script, String receiver, String search, int fromIndex) {
        if (script._getCancellationCheck() == null) {
            return receiver.indexOf(search, fromIndex);
        }
        return indexOfWithPolling(script, receiver, search, fromIndex);
    }

    /**
     * Cancellation-aware {@link String#lastIndexOf(String)}.  Symmetric to {@link #indexOf(PainlessScript, String, String)}: scans
     * candidate starting positions backwards from the end of the receiver, polling between each.
     */
    public static int lastIndexOf(PainlessScript script, String receiver, String search) {
        if (script._getCancellationCheck() == null) {
            return receiver.lastIndexOf(search);
        }
        return lastIndexOfWithPolling(script, receiver, search, receiver.length());
    }

    /**
     * Cancellation-aware {@link String#lastIndexOf(String, int)}.  Same as {@link #lastIndexOf(PainlessScript, String, String)} but
     * starts the backward search no later than {@code fromIndex}.
     */
    public static int lastIndexOf(PainlessScript script, String receiver, String search, int fromIndex) {
        if (script._getCancellationCheck() == null) {
            return receiver.lastIndexOf(search, fromIndex);
        }
        return lastIndexOfWithPolling(script, receiver, search, fromIndex);
    }

    /**
     * Cancellation-aware {@link String#contains(CharSequence)}.  Delegates to the indexOf augmentation, which polls between candidate
     * positions, so the contains check honours the search timeout even when the haystack and needle pathologically interact.
     */
    public static boolean contains(PainlessScript script, String receiver, CharSequence search) {
        if (script._getCancellationCheck() == null) {
            return receiver.contains(search);
        }
        return indexOfWithPolling(script, receiver, search.toString(), 0) >= 0;
    }

    /**
     * Naive forward scan with cancellation polling between candidate starting positions.  Matches the JDK's documented behaviour for
     * edge cases: empty {@code search} returns {@code min(fromIndex, length)}; negative {@code fromIndex} is clamped to zero; if the
     * needle cannot fit anywhere starting at or after {@code fromIndex}, returns {@code -1}.
     */
    private static int indexOfWithPolling(PainlessScript script, String receiver, String search, int fromIndex) {
        int receiverLength = receiver.length();
        int searchLength = search.length();
        int searchStart = Math.max(0, fromIndex);
        if (searchLength == 0) {
            return Math.min(searchStart, receiverLength);
        }
        int lastStart = receiverLength - searchLength;
        for (int start = searchStart; start <= lastStart; start++) {
            int offset = 0;
            while (offset < searchLength && receiver.charAt(start + offset) == search.charAt(offset)) {
                offset++;
            }
            if (offset == searchLength) {
                return start;
            }
            script._pollCancellation();
        }
        return -1;
    }

    /**
     * Naive backward scan with cancellation polling between candidate starting positions.  Matches the JDK's documented behaviour
     * for edge cases: a negative {@code fromIndex} returns {@code -1}; empty {@code search} returns {@code min(fromIndex, length)};
     * if there is no room for the needle anywhere on or before {@code fromIndex}, returns {@code -1}.
     */
    private static int lastIndexOfWithPolling(PainlessScript script, String receiver, String search, int fromIndex) {
        int receiverLength = receiver.length();
        int searchLength = search.length();
        if (fromIndex < 0) {
            return -1;
        }
        if (searchLength == 0) {
            return Math.min(fromIndex, receiverLength);
        }
        int searchStart = Math.min(fromIndex, receiverLength - searchLength);
        if (searchStart < 0) {
            return -1;
        }
        for (int start = searchStart; start >= 0; start--) {
            int offset = 0;
            while (offset < searchLength && receiver.charAt(start + offset) == search.charAt(offset)) {
                offset++;
            }
            if (offset == searchLength) {
                return start;
            }
            script._pollCancellation();
        }
        return -1;
    }

    /**
     * The initial size of the {@link StringBuilder} used for {@link #replaceFirst(CharSequence, int, Pattern, Function)} and
     * {@link #replaceAll(PainlessScript, CharSequence, int, Pattern, Function)} for a particular sequence. We ape
     * {{@link StringBuilder#StringBuilder(CharSequence)} here and add 16 extra chars to the buffer to have a little room for growth.
     */
    private static int initialBufferForReplaceWith(CharSequence seq) {
        return seq.length() + 16;
    }

    /**
     * Encode a String in Base64. Use {@link Base64.Encoder#encodeToString(byte[])} if you have to encode bytes rather than a string.
     */
    public static String encodeBase64(String receiver) {
        return Base64.getEncoder().encodeToString(receiver.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Decode some Base64 bytes and build a UTF-8 encoded string. Use {@link Base64.Decoder#decode(String)} if you'd prefer bytes to work
     * with bytes.
     */
    public static String decodeBase64(String receiver) {
        return new String(Base64.getDecoder().decode(receiver.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    /**
     * Split 'receiver' by 'token' as many times as possible..
     */
    public static String[] splitOnToken(String receiver, String token) {
        return splitOnToken(receiver, token, -1);
    }

    /**
     * Split 'receiver' by 'token' up to 'limit' times.  Any limit less than 1 is ignored.
     */
    public static String[] splitOnToken(String receiver, String token, int limit) {
        // Check if it's even possible to perform a split
        if (receiver == null || receiver.length() == 0 || token == null || token.length() == 0 || receiver.length() < token.length()) {
            return new String[] { receiver };
        }

        // List of string segments we have found
        ArrayList<String> result = new ArrayList<String>();

        // Keep track of where we are in the string
        // indexOf(tok, startPos) is faster than creating a new search context ever loop with substring(start, end)
        int pos = 0;

        // Loop until we hit the limit or forever if we are passed in less than one (signifying no limit)
        // If Integer.MIN_VALUE is passed in, it will still continue to loop down to 1 from MAX_VALUE
        // This edge case should be fine as we are limited by receiver length (Integer.MAX_VALUE) even if we split at every char
        for (; limit != 1; limit--) {

            // Find the next occurrence of token after current pos
            int idx = receiver.indexOf(token, pos);

            // Reached the end of the string without another match
            if (idx == -1) {
                break;
            }

            // Add the found segment to the result list
            result.add(receiver.substring(pos, idx));

            // Move our search position to the next possible location
            pos = idx + token.length();
        }
        // Add the remaining string to the result list
        result.add(receiver.substring(pos));

        // O(N) or faster depending on implementation
        return result.toArray(new String[0]);
    }

    /**
     * Access values in nested containers with a dot separated path.  Path elements are treated
     * as strings for Maps and integers for Lists.
     * @throws IllegalArgumentException if any of the following:
     *  - path is empty
     *  - path contains a trailing '.' or a repeated '.'
     *  - an element of the path does not exist, ie key or index not present
     *  - there is a non-container type at a non-terminal path element
     *  - a path element for a List is not an integer
     * @return object at path
     */
    public static <E> Object getByPath(List<E> receiver, String path) {
        return getByPathDispatch(receiver, splitPath(path), 0, throwCantFindValue(path));
    }

    /**
     * Same as {@link #getByPath(List, String)}, but for Map.
     */
    public static <K, V> Object getByPath(Map<K, V> receiver, String path) {
        return getByPathDispatch(receiver, splitPath(path), 0, throwCantFindValue(path));
    }

    /**
     * Same as {@link #getByPath(List, String)}, but with a default value.
     * @return element at path or {@code defaultValue} if the terminal path element does not exist.
     */
    public static <E> Object getByPath(List<E> receiver, String path, Object defaultValue) {
        return getByPathDispatch(receiver, splitPath(path), 0, () -> defaultValue);
    }

    /**
     * Same as {@link #getByPath(List, String, Object)}, but for Map.
     */
    public static <K, V> Object getByPath(Map<K, V> receiver, String path, Object defaultValue) {
        return getByPathDispatch(receiver, splitPath(path), 0, () -> defaultValue);
    }

    // Dispatches to getByPathMap, getByPathList or returns obj if done. See handleMissing for dealing with missing
    // elements.
    private static Object getByPathDispatch(Object obj, String[] elements, int i, Supplier<Object> defaultSupplier) {
        if (i > elements.length - 1) {
            return obj;
        } else if (elements[i].length() == 0) {
            String format = "Extra '.' in path [%s] at index [%d]";
            throw new IllegalArgumentException(String.format(Locale.ROOT, format, String.join(".", elements), i));
        } else if (obj instanceof Map<?, ?>) {
            return getByPathMap((Map<?, ?>) obj, elements, i, defaultSupplier);
        } else if (obj instanceof List<?>) {
            return getByPathList((List<?>) obj, elements, i, defaultSupplier);
        }
        return handleMissing(obj, elements, i, defaultSupplier);
    }

    // lookup existing key in map, call back to dispatch.
    private static <K, V> Object getByPathMap(Map<K, V> map, String[] elements, int i, Supplier<Object> defaultSupplier) {
        String element = elements[i];
        if (map.containsKey(element)) {
            return getByPathDispatch(map.get(element), elements, i + 1, defaultSupplier);
        }
        return handleMissing(map, elements, i, defaultSupplier);
    }

    // lookup existing index in list, call back to dispatch. Throws IllegalArgumentException with NumberFormatException
    // if index can't be parsed as an int.
    private static <E> Object getByPathList(List<E> list, String[] elements, int i, Supplier<Object> defaultSupplier) {
        String element = elements[i];
        try {
            int elemInt = Integer.parseInt(element);
            if (list.size() >= elemInt) {
                return getByPathDispatch(list.get(elemInt), elements, i + 1, defaultSupplier);
            }
        } catch (NumberFormatException e) {
            String format = "Could not parse [%s] as a int index into list at path [%s] and index [%d]";
            throw new IllegalArgumentException(String.format(Locale.ROOT, format, element, String.join(".", elements), i), e);
        }
        return handleMissing(list, elements, i, defaultSupplier);
    }

    // Split path on '.', throws IllegalArgumentException for empty paths and paths ending in '.'
    private static String[] splitPath(String path) {
        if (path.length() == 0) {
            throw new IllegalArgumentException("Missing path");
        }
        if (path.endsWith(".")) {
            String format = "Trailing '.' in path [%s]";
            throw new IllegalArgumentException(String.format(Locale.ROOT, format, path));
        }
        return path.split("\\.");
    }

    // A supplier that throws IllegalArgumentException
    private static Supplier<Object> throwCantFindValue(String path) {
        return () -> { throw new IllegalArgumentException(String.format(Locale.ROOT, "Could not find value at path [%s]", path)); };
    }

    // Use defaultSupplier if at last path element, otherwise throw IllegalArgumentException
    private static Object handleMissing(Object obj, String[] elements, int i, Supplier<Object> defaultSupplier) {
        if (obj instanceof List || obj instanceof Map) {
            if (elements.length - 1 == i) {
                return defaultSupplier.get();
            }
            String format = "Container does not have [%s], for non-terminal index [%d] in path [%s]";
            throw new IllegalArgumentException(String.format(Locale.ROOT, format, elements[i], i, String.join(".", elements)));
        }
        String format = "Non-container [%s] at [%s], index [%d] in path [%s]";
        throw new IllegalArgumentException(
            String.format(Locale.ROOT, format, obj.getClass().getName(), elements[i], i, String.join(".", elements))
        );
    }

    public static String sha1(String source) {
        return MessageDigests.toHexString(MessageDigests.sha1().digest(source.getBytes(StandardCharsets.UTF_8)));
    }

    public static String sha256(String source) {
        return MessageDigests.toHexString(MessageDigests.sha256().digest(source.getBytes(StandardCharsets.UTF_8)));
    }

    public static String sha512(String source) {
        return MessageDigests.toHexString(MessageDigests.sha512().digest(source.getBytes(StandardCharsets.UTF_8)));
    }

    public static final int UNLIMITED_PATTERN_FACTOR = 0;
    public static final int DISABLED_PATTERN_FACTOR = -1;

    // Regular Expression Pattern augmentations with limit factor injected
    public static String[] split(Pattern receiver, int limitFactor, CharSequence input) {
        if (limitFactor == UNLIMITED_PATTERN_FACTOR) {
            return receiver.split(input);
        }
        return receiver.split(new LimitedCharSequence(input, receiver, limitFactor));
    }

    public static String[] split(Pattern receiver, int limitFactor, CharSequence input, int limit) {
        if (limitFactor == UNLIMITED_PATTERN_FACTOR) {
            return receiver.split(input, limit);
        }
        return receiver.split(new LimitedCharSequence(input, receiver, limitFactor), limit);
    }

    public static Stream<String> splitAsStream(Pattern receiver, int limitFactor, CharSequence input) {
        if (limitFactor == UNLIMITED_PATTERN_FACTOR) {
            return receiver.splitAsStream(input);
        }
        return receiver.splitAsStream(new LimitedCharSequence(input, receiver, limitFactor));
    }

    public static Matcher matcher(Pattern receiver, int limitFactor, CharSequence input) {
        if (limitFactor == UNLIMITED_PATTERN_FACTOR) {
            return receiver.matcher(input);
        }
        return receiver.matcher(new LimitedCharSequence(input, receiver, limitFactor));
    }

    /**
     * Convert a {@link TemporalAccessor} into millis since epoch like {@link Instant#toEpochMilli()}.
     */
    public static long toEpochMilli(TemporalAccessor receiver) {
        return receiver.getLong(ChronoField.INSTANT_SECONDS) * 1_000 + receiver.get(ChronoField.NANO_OF_SECOND) / 1_000_000;
    }

    public static long getMillis(TemporalAccessor receiver) {
        return toEpochMilli(receiver);
    }

    public static DayOfWeek getDayOfWeekEnum(ZonedDateTime receiver) {
        return receiver.getDayOfWeek();
    }

    public static int getCenturyOfEra(ZonedDateTime receiver) {
        throw new UnsupportedOperationException(
            "[getCenturyOfEra] is no longer available; " + "use [get(ChronoField.YEAR_OF_ERA) / 100] instead"
        );
    }

    public static int getEra(ZonedDateTime receiver) {
        throw new UnsupportedOperationException("[getEra] is no longer available; use [get(ChronoField.ERA)] instead");
    }

    public static int getHourOfDay(ZonedDateTime receiver) {
        throw new UnsupportedOperationException("[getHourOfDay] is no longer available; use [getHour()] instead");
    }

    public static int getMillisOfDay(ZonedDateTime receiver) {
        throw new UnsupportedOperationException("[getMillisOfDay] is no longer available; use [get(ChronoField.MILLI_OF_DAY)] instead");
    }

    public static int getMillisOfSecond(ZonedDateTime receiver) {
        throw new UnsupportedOperationException(
            "[getMillisOfSecond] is no longer available; " + "use [get(ChronoField.MILLI_OF_SECOND)] instead"
        );
    }

    public static int getMinuteOfDay(ZonedDateTime receiver) {
        throw new UnsupportedOperationException(
            "[getMinuteOfDay] is no longer available; " + "use [get(ChronoField.MINUTE_OF_DAY)] instead"
        );
    }

    public static int getMinuteOfHour(ZonedDateTime receiver) {
        throw new UnsupportedOperationException("[getMinuteOfHour] is no longer available; use [getMinute()] instead");
    }

    public static int getMonthOfYear(ZonedDateTime receiver) {
        throw new UnsupportedOperationException("[getMonthOfYear] is no longer available; use [getMonthValue()] instead");
    }

    public static int getSecondOfDay(ZonedDateTime receiver) {
        throw new UnsupportedOperationException(
            "[getSecondOfDay] is no longer available; " + "use [get(ChronoField.SECOND_OF_DAY)] instead"
        );
    }

    public static int getSecondOfMinute(ZonedDateTime receiver) {
        throw new UnsupportedOperationException("[getSecondOfMinute] is no longer available; use [getSecond()] instead");
    }

    public static int getWeekOfWeekyear(ZonedDateTime receiver) {
        throw new UnsupportedOperationException(
            "[getWeekOfWeekyear] is no longer available; " + "use [get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)] instead"
        );
    }

    public static int getWeekyear(ZonedDateTime receiver) {
        throw new UnsupportedOperationException("[getWeekyear] is no longer available; use [get(IsoFields.WEEK_BASED_YEAR)] instead");
    }

    public static int getYearOfCentury(ZonedDateTime receiver) {
        throw new UnsupportedOperationException(
            "[getYearOfCentury] is no longer available; " + "use [get(ChronoField.YEAR_OF_ERA) % 100] instead"
        );
    }

    public static int getYearOfEra(ZonedDateTime receiver) {
        throw new UnsupportedOperationException("[getYearOfEra] is no longer available; use [get(ChronoField.YEAR_OF_ERA)] instead");
    }
}
