/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IteratorsTests extends ESTestCase {
    public void testConcatentation() {
        List<Integer> threeTwoOne = Arrays.asList(3, 2, 1);
        List<Integer> fourFiveSix = Arrays.asList(4, 5, 6);
        Iterator<Integer> concat = Iterators.concat(threeTwoOne.iterator(), fourFiveSix.iterator());
        assertContainsInOrder(concat, 3, 2, 1, 4, 5, 6);
    }

    public void testNoConcatenation() {
        Iterator<Integer> iterator = Iterators.<Integer>concat();
        assertEmptyIterator(iterator);
    }

    public void testEmptyConcatenation() {
        Iterator<Integer> iterator = Iterators.<Integer>concat(Collections.emptyIterator());
        assertEmptyIterator(iterator);
    }

    public void testMultipleEmptyConcatenation() {
        Iterator<Integer> iterator = Iterators.concat(Collections.emptyIterator(), Collections.emptyIterator());
        assertEmptyIterator(iterator);
    }

    public void testSingleton() {
        int value = randomInt();
        assertSingleton(value, singletonIterator(value));
    }

    public void testEmptyBeforeSingleton() {
        int value = randomInt();
        assertSingleton(value, Collections.emptyIterator(), singletonIterator(value));
    }

    public void testEmptyAfterSingleton() {
        int value = randomInt();
        assertSingleton(value, singletonIterator(value), Collections.emptyIterator());
    }

    public void testRandomSingleton() {
        int numberOfIterators = randomIntBetween(1, 1000);
        int singletonIndex = randomIntBetween(0, numberOfIterators - 1);
        int value = randomInt();
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Iterator<Integer>[] iterators = new Iterator[numberOfIterators];
        for (int i = 0; i < numberOfIterators; i++) {
            iterators[i] = i != singletonIndex ? Collections.emptyIterator() : singletonIterator(value);
        }
        assertSingleton(value, iterators);
    }

    public void testRandomIterators() {
        int numberOfIterators = randomIntBetween(1, 1000);
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Iterator<Integer>[] iterators = new Iterator[numberOfIterators];
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < numberOfIterators; i++) {
            int numberOfValues = randomIntBetween(0, 256);
            List<Integer> theseValues = new ArrayList<>();
            for (int j = 0; j < numberOfValues; j++) {
                int value = randomInt();
                values.add(value);
                theseValues.add(value);
            }
            iterators[i] = theseValues.iterator();
        }
        assertContainsInOrder(Iterators.concat(iterators), values.toArray(new Integer[values.size()]));
    }

    public void testTwoEntries() {
        int first = randomInt();
        int second = randomInt();
        Iterator<Integer> concat = Iterators.concat(
            singletonIterator(first),
            Collections.emptyIterator(),
            Collections.emptyIterator(),
            singletonIterator(second)
        );
        assertContainsInOrder(concat, first, second);
    }

    public void testNull() {
        try {
            Iterators.concat((Iterator<?>) null);
            fail("expected " + NullPointerException.class.getSimpleName());
        } catch (NullPointerException e) {

        }
    }

    public void testNullIterator() {
        try {
            Iterators.concat(singletonIterator(1), Collections.emptyIterator(), null, Collections.emptyIterator(), singletonIterator(2));
            fail("expected " + NullPointerException.class.getSimpleName());
        } catch (NullPointerException e) {

        }
    }

    public void testArrayIterator() {
        Integer[] array = randomIntegerArray();
        Iterator<Integer> iterator = Iterators.forArray(array);

        int i = 0;
        while (iterator.hasNext()) {
            assertEquals(array[i++], iterator.next());
        }
        assertEquals(array.length, i);
    }

    public void testArrayIteratorForEachRemaining() {
        Integer[] array = randomIntegerArray();
        Iterator<Integer> iterator = Iterators.forArray(array);

        AtomicInteger index = new AtomicInteger();
        iterator.forEachRemaining(i -> assertEquals(array[index.getAndIncrement()], i));
        assertEquals(array.length, index.get());
    }

    public void testArrayIteratorIsUnmodifiable() {
        Integer[] array = randomIntegerArray();
        Iterator<Integer> iterator = Iterators.forArray(array);

        expectThrows(UnsupportedOperationException.class, iterator::remove);
    }

    public void testArrayIteratorThrowsNoSuchElementExceptionWhenDepleted() {
        Integer[] array = randomIntegerArray();
        Iterator<Integer> iterator = Iterators.forArray(array);
        for (int i = 0; i < array.length; i++) {
            iterator.next();
        }

        expectThrows(NoSuchElementException.class, iterator::next);
    }

    public void testArrayIteratorOnNull() {
        expectThrows(NullPointerException.class, "Unable to iterate over a null array", () -> Iterators.forArray(null));
    }

    public void testForRange() {
        String[] array = generateRandomStringArray(20, 20, false, true);
        Iterator<String> iterator = Iterators.forRange(0, array.length, i -> array[i]);

        int i = 0;
        while (iterator.hasNext()) {
            assertEquals(array[i++], iterator.next());
        }
        assertEquals(array.length, i);
    }

    public void testForRangeOnNull() {
        expectThrows(NullPointerException.class, () -> Iterators.forRange(0, 1, null));
    }

    public void testFlatMap() {
        final var array = randomIntegerArray();
        assertEmptyIterator(Iterators.flatMap(Iterators.forArray(array), i -> Iterators.concat()));
        assertEmptyIterator(Iterators.flatMap(Iterators.concat(), i -> Iterators.single("foo")));

        final var index = new AtomicInteger();
        Iterators.flatMap(Iterators.forArray(array), Iterators::single)
            .forEachRemaining(i -> assertEquals(array[index.getAndIncrement()], i));
        assertEquals(array.length, index.get());

        index.set(0);
        Iterators.flatMap(Iterators.forArray(array), i -> Iterators.forArray(new Integer[] { i, i, i }))
            .forEachRemaining(i -> assertEquals(array[(index.getAndIncrement() / 3)], i));
        assertEquals(array.length * 3, index.get());

        final var input = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            IntStream.range(0, between(0, 2)).forEachOrdered(ignored -> input.add(0));
            input.add(i);
            IntStream.range(0, between(0, 2)).forEachOrdered(ignored -> input.add(0));
        }

        index.set(0);
        final var expectedArray = new Integer[] { 0, 0, 1, 0, 1, 2, 0, 1, 2, 3 };
        Iterators.flatMap(input.listIterator(), i -> IntStream.range(0, (int) i).iterator())
            .forEachRemaining(i -> assertEquals(expectedArray[(index.getAndIncrement())], i));
        assertEquals(expectedArray.length, index.get());
    }

    public void testMap() {
        assertEmptyIterator(Iterators.map(Iterators.concat(), i -> "foo"));

        final var array = randomIntegerArray();
        final var index = new AtomicInteger();
        Iterators.map(Iterators.forArray(array), i -> i * 2)
            .forEachRemaining(i -> assertEquals(array[index.getAndIncrement()] * 2, (long) i));
        assertEquals(array.length, index.get());
    }

    public void testFilter() {
        assertSame(Collections.emptyIterator(), Iterators.filter(Collections.emptyIterator(), i -> fail(null, "not called")));

        final var array = randomIntegerArray();
        assertSame(Collections.emptyIterator(), Iterators.filter(Iterators.forArray(array), i -> false));

        final var threshold = array.length > 0 && randomBoolean() ? randomFrom(array) : randomIntBetween(0, 1000);
        final Predicate<Integer> predicate = i -> i <= threshold;
        final var expectedResults = Arrays.stream(array).filter(predicate).toList();
        final var index = new AtomicInteger();
        Iterators.filter(Iterators.forArray(array), predicate)
            .forEachRemaining(i -> assertEquals(expectedResults.get(index.getAndIncrement()), i));

        if (Assertions.ENABLED) {
            final var predicateCalled = new AtomicBoolean();
            final var inputIterator = Iterators.forArray(new Object[] { null });
            expectThrows(AssertionError.class, () -> Iterators.filter(inputIterator, i -> predicateCalled.compareAndSet(false, true)));
            assertFalse(predicateCalled.get());
        }
    }

    public void testLimit() {
        var result = Iterators.limit(Collections.emptyIterator(), 10);
        assertThat(result.hasNext(), is(false));
        assertThat(Iterators.toList(result), is(empty()));

        var values = List.of(1, 2, 3);
        result = Iterators.limit(values.iterator(), 10);
        assertThat(result.hasNext(), is(true));
        assertThat(Iterators.toList(result), contains(1, 2, 3));

        result = Iterators.limit(values.iterator(), 2);
        assertThat(result.hasNext(), is(true));
        assertThat(Iterators.toList(result), contains(1, 2));

        result = Iterators.limit(values.iterator(), 0);
        assertThat(result.hasNext(), is(false));
        assertThat(Iterators.toList(result), is(empty()));
    }

    public void testFailFast() {
        final var array = randomIntegerArray();
        assertEmptyIterator(Iterators.failFast(Iterators.forArray(array), () -> true));

        final var index = new AtomicInteger();
        Iterators.failFast(Iterators.forArray(array), () -> false).forEachRemaining(i -> assertEquals(array[index.getAndIncrement()], i));
        assertEquals(array.length, index.get());

        final var isFailing = new AtomicBoolean();
        index.set(0);
        Iterators.failFast(Iterators.concat(Iterators.forArray(array), new Iterator<>() {
            @Override
            public boolean hasNext() {
                isFailing.set(true);
                return true;
            }

            @Override
            public Integer next() {
                return 0;
            }
        }), isFailing::get).forEachRemaining(i -> assertEquals(array[index.getAndIncrement()], i));
        assertEquals(array.length, index.get());
    }

    public void testEnumerate() {
        assertEmptyIterator(Iterators.enumerate(Iterators.concat(), Tuple::new));

        final var array = randomIntegerArray();
        final var index = new AtomicInteger();
        Iterators.enumerate(Iterators.forArray(array), Tuple::new).forEachRemaining(t -> {
            int idx = index.getAndIncrement();
            assertEquals(idx, t.v1().intValue());
            assertEquals(array[idx], t.v2());
        });
        assertEquals(array.length, index.get());
    }

    public void testSupplier() {
        assertEmptyIterator(Iterators.fromSupplier(() -> null));

        final var array = randomIntegerArray();
        final var index = new AtomicInteger();
        final var queue = new LinkedList<>(Arrays.asList(array));
        Iterators.fromSupplier(queue::pollFirst).forEachRemaining(i -> assertEquals(array[index.getAndIncrement()], i));
        assertEquals(array.length, index.get());
    }

    public void testCycle() {
        final List<Integer> source = List.of(1, 5, 100, 20);
        final Iterator<Integer> iterator = Iterators.cycling(source);
        assertThat(iterator.hasNext(), equalTo(true));
        for (int i = 0; i < 10; i++) {
            assertThat(iterator.hasNext(), equalTo(true));
            assertThat(iterator.next(), equalTo(1));
            assertThat(iterator.next(), equalTo(5));
            assertThat(iterator.next(), equalTo(100));
            assertThat(iterator.next(), equalTo(20));
        }
    }

    public void testEquals() {
        final BiPredicate<Object, Object> notCalled = (a, b) -> { throw new AssertionError("not called"); };

        assertTrue(Iterators.equals(null, null, notCalled));
        assertFalse(Iterators.equals(Collections.emptyIterator(), null, notCalled));
        assertFalse(Iterators.equals(null, Collections.emptyIterator(), notCalled));
        assertTrue(Iterators.equals(Collections.emptyIterator(), Collections.emptyIterator(), notCalled));

        assertFalse(Iterators.equals(Collections.emptyIterator(), List.of(1).iterator(), notCalled));
        assertFalse(Iterators.equals(List.of(1).iterator(), Collections.emptyIterator(), notCalled));
        assertTrue(Iterators.equals(List.of(1).iterator(), List.of(1).iterator(), Objects::equals));
        assertFalse(Iterators.equals(List.of(1).iterator(), List.of(2).iterator(), Objects::equals));
        assertFalse(Iterators.equals(List.of(1, 2).iterator(), List.of(1).iterator(), Objects::equals));
        assertFalse(Iterators.equals(List.of(1).iterator(), List.of(1, 2).iterator(), Objects::equals));

        final var strings1 = randomList(10, () -> randomAlphaOfLength(10));
        final var strings2 = new ArrayList<>(strings1);

        assertTrue(Iterators.equals(strings1.iterator(), strings2.iterator(), Objects::equals));

        if (strings2.size() == 0 || randomBoolean()) {
            strings2.add(randomAlphaOfLength(10));
        } else {
            final var index = between(0, strings2.size() - 1);
            if (randomBoolean()) {
                strings2.remove(index);
            } else {
                strings2.set(index, randomValueOtherThan(strings2.get(index), () -> randomAlphaOfLength(10)));
            }
        }
        assertFalse(Iterators.equals(strings1.iterator(), strings2.iterator(), Objects::equals));
    }

    public void testHashCode() {
        final ToIntFunction<Object> notCalled = (a) -> { throw new AssertionError("not called"); };
        assertEquals(0, Iterators.hashCode(null, notCalled));
        assertEquals(1, Iterators.hashCode(Collections.emptyIterator(), notCalled));

        final var numbers = randomIntegerArray();
        assertEquals(Arrays.hashCode(numbers), Iterators.hashCode(Arrays.stream(numbers).iterator(), Objects::hashCode));
    }

    private static Integer[] randomIntegerArray() {
        return Randomness.get().ints(randomIntBetween(0, 1000)).boxed().toArray(Integer[]::new);
    }

    private <T> Iterator<T> singletonIterator(T value) {
        return Collections.singleton(value).iterator();
    }

    @SafeVarargs
    @SuppressWarnings({ "unchecked", "varargs" })
    private <T> void assertSingleton(T value, Iterator<T>... iterators) {
        Iterator<T> concat = Iterators.concat(iterators);
        assertContainsInOrder(concat, value);
    }

    @SafeVarargs
    @SuppressWarnings({ "varargs" })
    private <T> void assertContainsInOrder(Iterator<T> iterator, T... values) {
        for (T value : values) {
            assertTrue(iterator.hasNext());
            assertEquals(value, iterator.next());
        }
        assertNoSuchElementException(iterator);
    }

    private <T> void assertEmptyIterator(Iterator<T> iterator) {
        assertFalse(iterator.hasNext());
        assertNoSuchElementException(iterator);
    }

    private <T> void assertNoSuchElementException(Iterator<T> iterator) {
        try {
            iterator.next();
            fail("expected " + NoSuchElementException.class.getSimpleName());
        } catch (NoSuchElementException e) {

        }
    }
}
