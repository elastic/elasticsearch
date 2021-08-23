/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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
        Iterator<Integer> iterator = Iterators.<Integer>concat(empty());
        assertEmptyIterator(iterator);
    }

    public void testMultipleEmptyConcatenation() {
        Iterator<Integer> iterator = Iterators.concat(empty(), empty());
        assertEmptyIterator(iterator);
    }

    public void testSingleton() {
        int value = randomInt();
        assertSingleton(value, singletonIterator(value));
    }

    public void testEmptyBeforeSingleton() {
        int value = randomInt();
        assertSingleton(value, empty(), singletonIterator(value));
    }


    public void testEmptyAfterSingleton() {
        int value = randomInt();
        assertSingleton(value, singletonIterator(value), empty());
    }

    public void testRandomSingleton() {
        int numberOfIterators = randomIntBetween(1, 1000);
        int singletonIndex = randomIntBetween(0, numberOfIterators - 1);
        int value = randomInt();
        @SuppressWarnings({"rawtypes", "unchecked"})
        Iterator<Integer>[] iterators = new Iterator[numberOfIterators];
        for (int i = 0; i < numberOfIterators; i++) {
            iterators[i] = i != singletonIndex ? empty() : singletonIterator(value);
        }
        assertSingleton(value, iterators);
    }

    public void testRandomIterators() {
        int numberOfIterators = randomIntBetween(1, 1000);
        @SuppressWarnings({"rawtypes", "unchecked"})
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
        Iterator<Integer> concat = Iterators.concat(singletonIterator(first), empty(), empty(), singletonIterator(second));
        assertContainsInOrder(concat, first, second);
    }

    public void testNull() {
        try {
            Iterators.concat((Iterator<?>)null);
            fail("expected " + NullPointerException.class.getSimpleName());
        } catch (NullPointerException e) {

        }
    }

    public void testNullIterator() {
        try {
            Iterators.concat(singletonIterator(1), empty(), null, empty(), singletonIterator(2));
            fail("expected " + NullPointerException.class.getSimpleName());
        } catch (NullPointerException e) {

        }
    }

    private <T> Iterator<T> singletonIterator(T value) {
        return Collections.singleton(value).iterator();
    }

    @SafeVarargs
    @SuppressWarnings({"unchecked", "varargs"})
    private final <T> void assertSingleton(T value, Iterator<T>... iterators) {
        Iterator<T> concat = Iterators.concat(iterators);
        assertContainsInOrder(concat, value);
    }

    private <T> Iterator<T> empty() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public T next() {
                throw new NoSuchElementException();
            }
        };
    }

    @SafeVarargs
    @SuppressWarnings({"varargs"})
    private final <T> void assertContainsInOrder(Iterator<T> iterator, T... values) {
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
