/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.RandomAccess;
import java.util.Set;
import java.util.UUID;

import static org.elasticsearch.common.util.CollectionUtils.DeepCopyOption.ORDERED;
import static org.elasticsearch.common.util.CollectionUtils.DeepCopyOption.UNMODIFIABLE;
import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.common.util.CollectionUtils.appendToCopyNoNullElements;
import static org.elasticsearch.common.util.CollectionUtils.arrayAsArrayList;
import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.common.util.CollectionUtils.deepCopy;
import static org.elasticsearch.common.util.CollectionUtils.eagerPartition;
import static org.elasticsearch.common.util.CollectionUtils.ensureNoSelfReferences;
import static org.elasticsearch.common.util.CollectionUtils.isEmpty;
import static org.elasticsearch.common.util.CollectionUtils.iterableAsArrayList;
import static org.elasticsearch.common.util.CollectionUtils.limitSize;
import static org.elasticsearch.common.util.CollectionUtils.newSingletonArrayList;
import static org.elasticsearch.common.util.CollectionUtils.sumIntValues;
import static org.elasticsearch.common.util.CollectionUtils.toArray;
import static org.elasticsearch.common.util.CollectionUtils.wrapUnmodifiableOrEmptySingleton;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class CollectionUtilsTests extends ESTestCase {

    /** Tests {@link CollectionUtils#isEmpty(Object[])} correctly identifies empty objects */
    public void testIsEmpty() {
        assertTrue(isEmpty(null));
        assertTrue(isEmpty(new Object[0]));

        final int arrayLength = randomIntBetween(1, 8);
        final Object[] objectArray = new Object[arrayLength];
        for (int elementIndex = 0; elementIndex < arrayLength; elementIndex++) {
            objectArray[elementIndex] = new Object();
        }
        assertFalse(isEmpty(objectArray));
    }

    /** Tests that an empty list is returned unchanged. */
    public void testRotateEmpty() {
        assertTrue(CollectionUtils.rotate(List.of(), randomInt()).isEmpty());
    }

    public void testRotate() {
        final int iters = scaledRandomIntBetween(10, 100);
        for (int k = 0; k < iters; ++k) {
            final int size = randomIntBetween(1, 100);
            final int distance = randomInt();
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < size; ++i) {
                list.add(new Object());
            }
            final List<Object> rotated = CollectionUtils.rotate(list, distance);
            // check content is the same
            assertEquals(rotated.size(), list.size());
            assertEquals(rotated.size(), list.size());
            assertEquals(new HashSet<>(rotated), new HashSet<>(list));
            // check stability
            for (int j = randomInt(4); j >= 0; --j) {
                assertEquals(rotated, CollectionUtils.rotate(list, distance));
            }
            // reverse
            if (distance != Integer.MIN_VALUE) {
                assertEquals(list, CollectionUtils.rotate(CollectionUtils.rotate(list, distance), -distance));
            }
        }
    }

    /** Non-{@link RandomAccess} lists should throw an {@link IllegalArgumentException}. */
    public void testRotateRejectsNonRandomAccessList() {
        final int listSize = randomIntBetween(2, 20);
        final LinkedList<String> linkedList = new LinkedList<>();
        for (int elementIndex = 0; elementIndex < listSize; elementIndex++) {
            linkedList.add(randomAlphaOfLength(4));
        }
        final int distance = randomInt();
        int offset = distance % listSize;
        if (offset < 0) {
            offset += listSize;
        }
        if (offset == 0) {
            assertSame(linkedList, CollectionUtils.rotate(linkedList, distance));
        } else {
            expectThrows(IllegalArgumentException.class, () -> CollectionUtils.rotate(linkedList, distance));
        }
    }

    private <T> void assertUniquify(List<T> list, Comparator<T> cmp, int size) {
        for (List<T> listCopy : List.of(new ArrayList<T>(list), new LinkedList<T>(list))) {
            CollectionUtils.uniquify(listCopy, cmp);
            for (int i = 0; i < listCopy.size() - 1; ++i) {
                assertThat(listCopy.get(i) + " < " + listCopy.get(i + 1), cmp.compare(listCopy.get(i), listCopy.get(i + 1)), lessThan(0));
            }
            assertThat(listCopy.size(), equalTo(size));
        }
    }

    public void testUniquify() {
        assertUniquify(List.<Integer>of(), Comparator.naturalOrder(), 0);
        assertUniquify(List.of(1), Comparator.naturalOrder(), 1);
        assertUniquify(List.of(1, 2, 3), Comparator.naturalOrder(), 3);
        assertUniquify(List.of(1, 1, 3), Comparator.naturalOrder(), 2);
        assertUniquify(List.of(1, 1, 1), Comparator.naturalOrder(), 1);
        assertUniquify(List.of(1, 2, 2, 3), Comparator.naturalOrder(), 3);
        assertUniquify(List.of(1, 2, 2, 2), Comparator.naturalOrder(), 2);
        assertUniquify(List.of(1, 2, 2, 3, 3, 5), Comparator.naturalOrder(), 4);

        for (int i = 0; i < 10; ++i) {
            int uniqueItems = randomIntBetween(1, 10);
            var list = new ArrayList<Integer>();
            int next = 1;
            for (int j = 0; j < uniqueItems; ++j) {
                int occurences = randomIntBetween(1, 10);
                while (occurences-- > 0) {
                    list.add(next);
                }
                next++;
            }
            assertUniquify(list, Comparator.naturalOrder(), uniqueItems);
        }
    }

    public void testToArray() {
        final int valueCount = randomIntBetween(0, 30);
        final List<Integer> integerList = new ArrayList<>();
        int expectedSum = 0;
        for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
            final int nextValue = randomIntBetween(1, 100);
            integerList.add(nextValue);
            expectedSum += nextValue;
        }
        int[] asArray = toArray(integerList);
        assertThat(asArray.length, equalTo(valueCount));
        assertThat(Arrays.stream(asArray).sum(), equalTo(expectedSum));

        final Set<Integer> integerSet = new HashSet<>(integerList);
        asArray = toArray(integerSet);
        assertThat(asArray.length, equalTo(integerSet.size()));
    }

    public void testToArrayWithNullCollection() {
        final NullPointerException nullPointer = expectThrows(NullPointerException.class, () -> toArray(null));
        assertNotNull(nullPointer);
    }

    /** {@link CollectionUtils#sumIntValues(Map)}: null, empty, and randomized maps sum to expected totals. */
    public void testSumIntValues() {
        assertThat(sumIntValues(null), equalTo(0));

        final Map<String, Integer> emptyMap = new HashMap<>();
        assertThat(sumIntValues(emptyMap), equalTo(0));

        final int entryCount = randomIntBetween(0, 25);
        final Map<String, Integer> randomMap = new HashMap<>();
        int expectedSum = 0;
        for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
            final int value = randomIntBetween(1, 100);
            expectedSum += value;
            randomMap.put(randomAlphanumericOfLength(randomIntBetween(1, 10)) + entryIndex, value);
        }
        assertThat(sumIntValues(randomMap), equalTo(expectedSum));
    }

    public void testEmptyPartition() {
        assertEquals(List.of(), eagerPartition(List.of(), 1));
    }

    public void testSimplePartition() {
        assertEquals(List.of(List.of(1, 2), List.of(3, 4), List.of(5)), eagerPartition(List.of(1, 2, 3, 4, 5), 2));
    }

    public void testSingletonPartition() {
        assertEquals(List.of(List.of(1), List.of(2), List.of(3), List.of(4), List.of(5)), eagerPartition(List.of(1, 2, 3, 4, 5), 1));
    }

    public void testOversizedPartition() {
        assertEquals(List.of(List.of(1, 2, 3, 4, 5)), eagerPartition(List.of(1, 2, 3, 4, 5), 15));
    }

    public void testPerfectPartition() {
        assertEquals(
            List.of(List.of(1, 2, 3, 4, 5, 6), List.of(7, 8, 9, 10, 11, 12)),
            eagerPartition(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), 6)
        );
    }

    public void testEnsureNoSelfReferences() {
        ensureNoSelfReferences("string", randomMessageHintOrNull("test with a string"));
        ensureNoSelfReferences(2, randomMessageHintOrNull("test with a number"));
        ensureNoSelfReferences(true, randomMessageHintOrNull("test with a boolean"));
        ensureNoSelfReferences(Map.of(), randomMessageHintOrNull("test with an empty map"));
        ensureNoSelfReferences(Set.of(), randomMessageHintOrNull("test with an empty set"));
        ensureNoSelfReferences(List.of(), randomMessageHintOrNull("test with an empty list"));
        ensureNoSelfReferences(new Object[0], randomMessageHintOrNull("test with an empty array"));
        ensureNoSelfReferences((Iterable<?>) Collections::emptyIterator, randomMessageHintOrNull("test with an empty iterable"));
    }

    /**
     * Two non-empty maps that reference each other (A → B → A) must be detected by identity.
     */
    public void testEnsureNoSelfReferencesMutualMaps() {
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        a.put("toB", b);
        b.put("toA", a);
        String hint = randomMessageHintOrNull("mutual maps");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ensureNoSelfReferences(a, hint));
        assertSelfReferenceMessage(e, hint);
    }

    /** Non-empty {@link Object Object[]} graphs without self-reference should complete without throwing. */
    public void testEnsureNoSelfReferencesNonEmptyObjectArrays() {
        String hint = randomMessageHintOrNull("object array");
        ensureNoSelfReferences(new Object[] { "a", 1L, true }, hint);
        ensureNoSelfReferences(new Object[] { Map.of("k", "v") }, hint);
        ensureNoSelfReferences(new Object[] { List.of(1, 2), new Object[] { "nested" } }, hint);
        ensureNoSelfReferences(new Object[] { new Object[] { new Object[] { "deep" } } }, hint);
        ensureNoSelfReferences(new Object[] { null, "x" }, hint);
    }

    /**
     * {@link Path} implements {@link Iterable} but must not be traversed as a generic iterable (or stack could blow).
     */
    public void testEnsureNoSelfReferencesPathNotWalkedAsIterable() {
        String hint = randomMessageHintOrNull("path");
        Path path = PathUtils.get("tmp", "segment", "file");
        ensureNoSelfReferences(path, hint);
        ensureNoSelfReferences(List.of(path, PathUtils.get("a")), hint);
        ensureNoSelfReferences(Map.of("p", path), hint);
        ensureNoSelfReferences(new Object[] { path, "s" }, hint);
    }

    /**
     * Values that are not maps, iterable collections, or arrays are treated as leaves (no traversal).
     */
    public void testEnsureNoSelfReferencesOpaqueTypes() {
        String hint = randomMessageHintOrNull("opaque");
        ensureNoSelfReferences(Instant.now(), hint);
        ensureNoSelfReferences(UUID.randomUUID(), hint);
        ensureNoSelfReferences(Locale.ROOT, hint);
        ensureNoSelfReferences(Optional.of("v"), hint);
        ensureNoSelfReferences(Optional.empty(), hint);
        ensureNoSelfReferences(Character.valueOf('z'), hint);
        ensureNoSelfReferences(new StringBuilder("sb"), hint);
        ensureNoSelfReferences(new Object(), hint);
        ensureNoSelfReferences(SampleEnum.X, hint);
        // primitive arrays are not Object[]; ensure we do not recurse into them (same as other opaque)
        ensureNoSelfReferences(new byte[] { 1, 2, 3 }, hint);
        ensureNoSelfReferences(new int[] { 1, 2, 3 }, hint);
        ensureNoSelfReferences(new long[] { 1L }, hint);
        ensureNoSelfReferences(new char[] { 'a', 'b' }, hint);
        ensureNoSelfReferences(new Object[] { new byte[] { 1 } }, hint);
    }

    /**
     * Randomly builds three levels of nesting (map, list, or array at each level). Runs an acyclic half and a
     * cyclic half: when embedding {@code root} at the deepest leaf, {@link CollectionUtils#ensureNoSelfReferences}
     * skips empty maps, so a back-reference to {@code root} embedded while it was still empty can fail to produce an
     * {@link IllegalArgumentException}. We pad before building and add a direct {@code root} self-entry so the cyclic
     * case always fails deterministically while still exercising the random deep shape under {@code "top"}.
     */
    public void testEnsureNoSelfReferencesRandomDepthThree() {
        final int iterations = scaledRandomIntBetween(20, 80);
        for (int i = 0; i < iterations; i++) {
            Map<String, Object> root = new HashMap<>();
            Object nested = buildRandomNested(3, root, false);
            root.put("top", nested);
            String hint = randomMessageHintOrNull("random depth three acyclic");
            ensureNoSelfReferences(root, hint);
        }
        for (int i = 0; i < iterations; i++) {
            Map<String, Object> root = new HashMap<>();
            root.put("pad", randomAlphaOfLength(3));
            Object nested = buildRandomNested(3, root, true);
            root.put("top", nested);
            // Direct identity cycle: always observable regardless of deep shape or empty-map short-circuit
            root.put("closure", root);
            String hint = randomMessageHintOrNull("random depth three cyclic");
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ensureNoSelfReferences(root, hint));
            assertSelfReferenceMessage(e, hint);
        }
    }

    private Object buildRandomNested(int depthRemaining, Object cycleTarget, boolean injectCycle) {
        if (depthRemaining == 0) {
            return injectCycle ? cycleTarget : randomAlphaOfLength(5);
        }
        Object child = buildRandomNested(depthRemaining - 1, cycleTarget, injectCycle);
        int kind = randomIntBetween(0, 2);
        return switch (kind) {
            case 0 -> {
                Map<String, Object> m = new HashMap<>();
                m.put("k", child);
                yield m;
            }
            case 1 -> {
                List<Object> list = new ArrayList<>();
                list.add(child);
                yield list;
            }
            case 2 -> new Object[] { child };
            default -> throw new AssertionError("unexpected kind value (not 0..2): " + kind);
        };
    }

    public void testEnsureNoSelfReferencesMap() {
        // map value
        {
            Map<String, Object> map = new HashMap<>();
            map.put("field", map);

            String hint = randomMessageHintOrNull("test with self ref value");
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ensureNoSelfReferences(map, hint));
            assertSelfReferenceMessage(e, hint);
        }
        // map key
        {
            Map<Object, Object> map = new HashMap<>();
            map.put(map, 1);

            String hint = randomMessageHintOrNull("test with self ref key");
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ensureNoSelfReferences(map, hint));
            assertSelfReferenceMessage(e, hint);
        }
        // nested map value
        {
            Map<String, Object> map = new HashMap<>();
            map.put("field", Set.of(List.of((Iterable<?>) () -> Iterators.single(new Object[] { map }))));

            String hint = randomMessageHintOrNull("test with self ref nested value");
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ensureNoSelfReferences(map, hint));
            assertSelfReferenceMessage(e, hint);
        }
    }

    public void testEnsureNoSelfReferencesSet() {
        Set<Object> set = new HashSet<>();
        set.add("foo");
        set.add(set);

        String hint = randomMessageHintOrNull("test with self ref set");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ensureNoSelfReferences(set, hint));
        assertSelfReferenceMessage(e, hint);
    }

    public void testEnsureNoSelfReferencesList() {
        List<Object> list = new ArrayList<>();
        list.add("foo");
        list.add(list);

        String hint = randomMessageHintOrNull("test with self ref list");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ensureNoSelfReferences(list, hint));
        assertSelfReferenceMessage(e, hint);
    }

    public void testEnsureNoSelfReferencesArray() {
        Object[] array = new Object[2];
        array[0] = "foo";
        array[1] = array;

        String hint = randomMessageHintOrNull("test with self ref array");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ensureNoSelfReferences(array, hint));
        assertSelfReferenceMessage(e, hint);
    }

    public void testIterableAsArrayListRejectsNullElements() {
        final NullPointerException nullPointer = expectThrows(NullPointerException.class, () -> iterableAsArrayList(null));
        assertEquals("elements", nullPointer.getMessage());
    }

    /**
     * When the iterable is also a {@link Collection}, a new {@link ArrayList} is returned.
     */
    public void testIterableAsArrayListWhenElementsIsCollection() {
        final int elementCount = randomIntBetween(0, 20);
        final List<String> backingList = new ArrayList<>();
        for (int elementIndex = 0; elementIndex < elementCount; elementIndex++) {
            backingList.add(randomAlphaOfLength(5));
        }
        final ArrayList<String> fromCollection = iterableAsArrayList(backingList);
        assertThat(fromCollection, equalTo(new ArrayList<>(backingList)));
        if (elementCount > 0) {
            assertNotSame(fromCollection, backingList);
        }
    }

    /**
     * When the iterable is not a {@link Collection}, elements are copied by iterating.
     */
    public void testIterableAsArrayListWhenElementsIsNotCollection() {
        final int elementCount = randomIntBetween(0, 20);
        final List<String> backingList = new ArrayList<>();
        for (int elementIndex = 0; elementIndex < elementCount; elementIndex++) {
            backingList.add(randomAlphaOfLength(5));
        }
        final Iterable<String> nonCollectionIterable = () -> {
            final Iterator<String> sourceIterator = backingList.iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return sourceIterator.hasNext();
                }

                @Override
                public String next() {
                    return sourceIterator.next();
                }
            };
        };
        final ArrayList<String> fromNonCollectionIterable = iterableAsArrayList(nonCollectionIterable);
        assertThat(fromNonCollectionIterable, equalTo(backingList));
    }

    public void testArrayAsArrayListWithNullElements() {
        final NullPointerException nullPointer = expectThrows(NullPointerException.class, () -> arrayAsArrayList((String[]) null));
        assertEquals("elements", nullPointer.getMessage());
    }

    public void testArrayAsArrayList() {
        final int elementCount = randomIntBetween(0, 20);
        final String[] stringArray = new String[elementCount];
        for (int elementIndex = 0; elementIndex < elementCount; elementIndex++) {
            stringArray[elementIndex] = randomAlphaOfLength(4);
        }
        final ArrayList<String> asArrayList = arrayAsArrayList(stringArray);
        assertThat(asArrayList, equalTo(new ArrayList<>(List.of(stringArray))));
        asArrayList.add("mutation-check");
    }

    public void testAppendToCopy() {
        final int originalListSize = randomIntBetween(0, 15);
        final List<String> originalList = new ArrayList<>();
        for (int elementIndex = 0; elementIndex < originalListSize; elementIndex++) {
            originalList.add(randomAlphaOfLength(3));
        }
        final String appendedElement = randomAlphaOfLength(4);
        final List<String> appended = appendToCopy(originalList, appendedElement);
        final List<String> expected = new ArrayList<>(originalList);
        expected.add(appendedElement);
        assertThat(appended, equalTo(expected));
        expectThrows(UnsupportedOperationException.class, () -> appended.add("mutate"));
    }

    public void testAppendToCopyNoNullElementsSingleArgOverload() {
        final int originalListSize = randomIntBetween(0, 12);
        final List<String> originalList = new ArrayList<>();
        for (int elementIndex = 0; elementIndex < originalListSize; elementIndex++) {
            originalList.add(randomAlphaOfLength(4));
        }
        final String singleExtra = randomAlphaOfLength(5);
        final List<String> withSingle = appendToCopyNoNullElements(originalList, singleExtra);
        final List<String> withVarargOne = appendToCopyNoNullElements(originalList, new String[] { singleExtra });
        assertThat(withSingle, equalTo(concatLists(originalList, List.of(singleExtra))));
        assertThat(withVarargOne, equalTo(withSingle));
    }

    public void testAppendToCopyNoNullElements() {
        final List<String> oldList = randomList(3, 3, () -> randomAlphaOfLength(10));
        final String[] extraElements = randomArray(2, 4, String[]::new, () -> randomAlphaOfLength(10));
        final List<String> newList = appendToCopyNoNullElements(oldList, extraElements);
        assertThat(newList, equalTo(concatLists(oldList, List.of(extraElements))));
    }

    public void testNewSingletonArrayList() {
        final String singleElement = randomAlphaOfLength(6);
        final ArrayList<String> singletonList = newSingletonArrayList(singleElement);
        assertThat(singletonList.size(), equalTo(1));
        assertThat(singletonList.getFirst(), equalTo(singleElement));
    }

    /** Null list throws {@link NullPointerException}. */
    public void testEagerPartitionRejectsNullList() {
        final NullPointerException nullPointer = expectThrows(NullPointerException.class, () -> eagerPartition(null, 1));
        assertEquals("list", nullPointer.getMessage());
    }

    /** Non-positive chunk size throws {@link IllegalArgumentException}. */
    public void testEagerPartitionRejectsNonPositiveSize() {
        for (int nonPositive : List.of(0, -1, Integer.MIN_VALUE)) {
            final IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> eagerPartition(List.of(1, 2), nonPositive)
            );
            assertEquals("size <= 0", exception.getMessage());
        }
    }

    public void testEagerPartitionRandomizedMatchesSimpleChunking() {
        final int totalElements = randomIntBetween(0, 100);
        final int chunkSize = randomIntBetween(1, 20);
        final List<String> sourceList = randomList(totalElements, totalElements, () -> randomAlphaOfLength(2));
        final List<List<String>> actualPartitions = eagerPartition(sourceList, chunkSize);
        final List<List<String>> expectedPartitions = new ArrayList<>();
        for (int startIndex = 0; startIndex < totalElements; startIndex += chunkSize) {
            final int endIndex = Math.min(startIndex + chunkSize, sourceList.size());
            expectedPartitions.add(new ArrayList<>(sourceList.subList(startIndex, endIndex)));
        }
        assertThat(actualPartitions.size(), equalTo(expectedPartitions.size()));
        for (int partitionIndex = 0; partitionIndex < expectedPartitions.size(); partitionIndex++) {
            assertThat(actualPartitions.get(partitionIndex), equalTo(expectedPartitions.get(partitionIndex)));
        }
    }

    public void testConcatLists() {
        final List<String> firstList = List.of("a", "b");
        final List<String> secondList = List.of("c", "d");
        assertThat(concatLists(firstList, secondList), equalTo(List.of("a", "b", "c", "d")));

        final int firstSize = randomIntBetween(0, 12);
        final int secondSize = randomIntBetween(0, 12);
        final List<Integer> firstRandom = randomList(firstSize, firstSize, ESTestCase::randomInt);
        final List<Integer> secondRandom = randomList(secondSize, secondSize, ESTestCase::randomInt);
        final List<Integer> concatenated = concatLists(firstRandom, secondRandom);
        assertThat(concatenated.size(), equalTo(firstSize + secondSize));
        for (int index = 0; index < firstSize; index++) {
            assertThat(concatenated.get(index), equalTo(firstRandom.get(index)));
        }
        for (int index = 0; index < secondSize; index++) {
            assertThat(concatenated.get(firstSize + index), equalTo(secondRandom.get(index)));
        }

    }

    public void testWrapUnmodifiableOrEmptySingleton() {
        assertThat(wrapUnmodifiableOrEmptySingleton(new ArrayList<>()).size(), equalTo(0));
        final List<String> withOneElement = new ArrayList<>();
        withOneElement.add("x");
        final List<String> unmodifiableNonEmpty = wrapUnmodifiableOrEmptySingleton(withOneElement);
        expectThrows(UnsupportedOperationException.class, () -> unmodifiableNonEmpty.add("y"));
        assertTrue(unmodifiableNonEmpty.contains("x"));
    }

    public void testLimitSizeOfShortList() {
        var shortList = randomList(0, 10, () -> "item");
        assertThat(limitSize(shortList, 10), equalTo(shortList));
    }

    public void testLimitSizeOfLongList() {
        var longList = randomList(10, 100, () -> "item");
        assertThat(limitSize(longList, 10), equalTo(longList.subList(0, 10)));
    }

    private enum SampleEnum {
        X
    }

    /** Randomly returns {@code null} or {@code messageWhenNonNull} to cover both exception message shapes. */
    private static String randomMessageHintOrNull(String messageWhenNonNull) {
        return randomBoolean() ? null : messageWhenNonNull;
    }

    private static void assertSelfReferenceMessage(IllegalArgumentException e, String hint) {
        if (hint == null) {
            assertEquals("Iterable object is self-referencing itself", e.getMessage());
        } else {
            assertEquals("Iterable object is self-referencing itself (" + hint + ")", e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // deepCopy tests
    // -------------------------------------------------------------------------

    public void testDeepCopyNull() {
        assertNull(deepCopy(null));
        assertNull(deepCopy(null, UNMODIFIABLE));
        assertNull(deepCopy(null, ORDERED));
        assertNull(deepCopy(null, UNMODIFIABLE, ORDERED));
    }

    public void testDeepCopyImmutableScalarsReturnSameReference() {
        String str = randomAlphaOfLength(5);
        assertSame(str, deepCopy(str));

        Boolean bool = randomBoolean();
        assertSame(bool, deepCopy(bool));

        Character ch = (char) randomIntBetween('a', 'z');
        assertSame(ch, deepCopy(ch));

        Byte b = (byte) randomIntBetween(0, 127);
        assertSame(b, deepCopy(b));

        Short s = (short) randomIntBetween(0, 1000);
        assertSame(s, deepCopy(s));

        Integer i = randomInt();
        assertSame(i, deepCopy(i));

        Long l = randomLong();
        assertSame(l, deepCopy(l));

        Float f = randomFloat();
        assertSame(f, deepCopy(f));

        Double d = randomDouble();
        assertSame(d, deepCopy(d));

        BigInteger bi = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        assertSame(bi, deepCopy(bi));

        BigDecimal bd = new BigDecimal("1.23456789012345678901234567890");
        assertSame(bd, deepCopy(bd));

        ZonedDateTime zdt = ZonedDateTime.now(ZoneOffset.UTC);
        assertSame(zdt, deepCopy(zdt));
    }

    public void testDeepCopyImmutableScalarsWithOptions() {
        // options do not affect pass-through of immutable scalars
        String str = randomAlphaOfLength(5);
        assertSame(str, deepCopy(str, UNMODIFIABLE));
        assertSame(str, deepCopy(str, ORDERED));
        assertSame(str, deepCopy(str, UNMODIFIABLE, ORDERED));
    }

    public void testDeepCopyDateClonesReference() {
        Date original = new Date(randomLong());
        Date copy = deepCopy(original);
        assertNotSame(original, copy);
        assertEquals(original, copy);
        // mutating the copy must not affect the original
        copy.setTime(original.getTime() + 1);
        assertNotEquals(original, copy);
    }

    public void testDeepCopyMap() {
        Map<String, Object> original = new HashMap<>();
        original.put("str", "hello");
        original.put("num", 42);
        original.put("nested", new HashMap<>(Map.of("k", "v")));

        Map<String, Object> copy = deepCopy(original);
        assertNotSame(original, copy);
        assertEquals(original, copy);
    }

    public void testDeepCopyMapMutationIsolation() {
        Map<String, Object> original = new HashMap<>();
        original.put("key", "value");

        Map<String, Object> copy = deepCopy(original);
        copy.put("extra", "added");
        assertFalse(original.containsKey("extra"));
    }

    public void testDeepCopyMapEmptyMap() {
        Map<String, Object> original = new HashMap<>();
        Map<String, Object> copy = deepCopy(original);
        assertNotSame(original, copy);
        assertTrue(copy.isEmpty());
    }

    public void testDeepCopyList() {
        List<Object> original = new ArrayList<>(List.of("a", 1, true));
        List<Object> copy = deepCopy(original);
        assertNotSame(original, copy);
        assertEquals(original, copy);
        copy.add("extra");
        assertFalse(original.contains("extra"));
    }

    public void testDeepCopyListEmptyList() {
        List<Object> original = new ArrayList<>();
        List<Object> copy = deepCopy(original);
        assertNotSame(original, copy);
        assertTrue(copy.isEmpty());
    }

    public void testDeepCopySet() {
        Set<Object> original = new HashSet<>(Set.of("x", "y", "z"));
        Set<Object> copy = deepCopy(original);
        assertNotSame(original, copy);
        assertEquals(original, copy);
        copy.add("extra");
        assertFalse(original.contains("extra"));
    }

    public void testDeepCopyByteArray() {
        byte[] original = new byte[] { 1, 2, 3, 4, 5 };
        byte[] copy = deepCopy(original);
        assertNotSame(original, copy);
        assertArrayEquals(original, copy);
        copy[0] = (byte) ~copy[0];
        assertFalse(copy[0] == original[0]);
    }

    public void testDeepCopyDoubleArray() {
        double[] original = new double[] { 1.0, 2.5, 3.14 };
        double[] copy = deepCopy(original);
        assertNotSame(original, copy);
        assertArrayEquals(original, copy, 0.0);
        copy[0] = 99.0;
        assertNotEquals(99.0, original[0]);
    }

    public void testDeepCopyDouble2DArray() {
        double[][] original = new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 } };
        double[][] copy = deepCopy(original);
        assertNotSame(original, copy);
        // outer array is a new instance
        assertNotSame(original[0], copy[0]);
        assertNotSame(original[1], copy[1]);
        assertArrayEquals(original[0], copy[0], 0.0);
        assertArrayEquals(original[1], copy[1], 0.0);
        // mutating the inner copy must not affect the original
        copy[0][0] = 99.0;
        assertNotEquals(99.0, original[0][0]);
    }

    public void testDeepCopyDouble2DArrayWithEmptyRows() {
        double[][] original = new double[][] { {}, { 1.0 } };
        double[][] copy = deepCopy(original);
        assertNotSame(original, copy);
        assertArrayEquals(original[0], copy[0], 0.0);
        assertArrayEquals(original[1], copy[1], 0.0);
    }

    public void testDeepCopyNestedMapInList() {
        Map<String, Object> inner = new HashMap<>(Map.of("key", "value"));
        List<Object> list = new ArrayList<>(List.of(inner));
        Map<String, Object> outer = new HashMap<>(Map.of("list", list));

        Map<String, Object> copy = deepCopy(outer);
        assertNotSame(outer, copy);

        @SuppressWarnings("unchecked")
        List<Object> copiedList = (List<Object>) copy.get("list");
        assertNotSame(list, copiedList);

        @SuppressWarnings("unchecked")
        Map<String, Object> copiedInner = (Map<String, Object>) copiedList.get(0);
        assertNotSame(inner, copiedInner);
        assertEquals("value", copiedInner.get("key"));

        // mutating the nested copy must not affect the original
        copiedInner.put("new", "thing");
        assertFalse(inner.containsKey("new"));
    }

    public void testDeepCopyNestedListInMap() {
        List<Object> innerList = new ArrayList<>(List.of(1, 2, 3));
        Map<String, Object> original = new HashMap<>(Map.of("items", innerList));

        Map<String, Object> copy = deepCopy(original);

        @SuppressWarnings("unchecked")
        List<Object> copiedList = (List<Object>) copy.get("items");
        assertNotSame(innerList, copiedList);
        copiedList.add(4);
        assertFalse(innerList.contains(4));
    }

    public void testDeepCopyUnmodifiableMap() {
        Map<String, Object> original = new HashMap<>(Map.of("k", "v"));
        Map<String, Object> copy = deepCopy(original, UNMODIFIABLE);
        assertNotSame(original, copy);
        assertEquals(original, copy);
        expectThrows(UnsupportedOperationException.class, () -> copy.put("new", "val"));
        expectThrows(UnsupportedOperationException.class, () -> copy.remove("k"));
    }

    public void testDeepCopyUnmodifiableList() {
        List<Object> original = new ArrayList<>(List.of(1, 2, 3));
        List<Object> copy = deepCopy(original, UNMODIFIABLE);
        assertNotSame(original, copy);
        assertEquals(original, copy);
        expectThrows(UnsupportedOperationException.class, () -> copy.add(4));
        expectThrows(UnsupportedOperationException.class, () -> copy.remove(0));
    }

    public void testDeepCopyUnmodifiableSet() {
        Set<Object> original = new HashSet<>(Set.of("a", "b"));
        Set<Object> copy = deepCopy(original, UNMODIFIABLE);
        assertNotSame(original, copy);
        assertEquals(original, copy);
        expectThrows(UnsupportedOperationException.class, () -> copy.add("c"));
    }

    public void testDeepCopyUnmodifiableNestedCollections() {
        List<Object> innerList = new ArrayList<>(List.of(10, 20));
        Map<String, Object> original = new HashMap<>();
        original.put("list", innerList);

        Map<String, Object> copy = deepCopy(original, UNMODIFIABLE);
        expectThrows(UnsupportedOperationException.class, () -> copy.put("k", "v"));

        @SuppressWarnings("unchecked")
        List<Object> copiedList = (List<Object>) copy.get("list");
        expectThrows(UnsupportedOperationException.class, () -> copiedList.add(30));
    }

    public void testDeepCopyUnmodifiableDoesNotAffectOriginal() {
        Map<String, Object> original = new HashMap<>(Map.of("k", "v"));
        deepCopy(original, UNMODIFIABLE);
        // the original must still be mutable
        original.put("k2", "v2");
        assertTrue(original.containsKey("k2"));
    }

    public void testDeepCopyOrderedPreservesMapInsertionOrder() {
        Map<String, Object> original = new LinkedHashMap<>();
        original.put("charlie", 3);
        original.put("alpha", 1);
        original.put("bravo", 2);

        Map<String, Object> copy = deepCopy(original, ORDERED);
        assertNotSame(original, copy);
        assertEquals(List.of("charlie", "alpha", "bravo"), new ArrayList<>(copy.keySet()));
    }

    public void testDeepCopyOrderedPreservesSetInsertionOrder() {
        Set<Object> original = new LinkedHashSet<>();
        original.add("charlie");
        original.add("alpha");
        original.add("bravo");

        Set<Object> copy = deepCopy(original, ORDERED);
        assertNotSame(original, copy);
        assertEquals(List.of("charlie", "alpha", "bravo"), new ArrayList<>(copy));
    }

    public void testDeepCopyOrderedNested() {
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("z", 1);
        inner.put("a", 2);
        Map<String, Object> outer = new LinkedHashMap<>();
        outer.put("inner", inner);
        outer.put("other", "val");

        Map<String, Object> copy = deepCopy(outer, ORDERED);
        assertEquals(List.of("inner", "other"), new ArrayList<>(copy.keySet()));

        @SuppressWarnings("unchecked")
        Map<String, Object> copiedInner = (Map<String, Object>) copy.get("inner");
        assertEquals(List.of("z", "a"), new ArrayList<>(copiedInner.keySet()));
    }

    public void testDeepCopyOrderedNestedSet() {
        // ORDERED should propagate into sets nested inside maps
        Set<Object> innerSet = new LinkedHashSet<>();
        innerSet.add("charlie");
        innerSet.add("alpha");
        innerSet.add("bravo");
        Map<String, Object> outer = new LinkedHashMap<>();
        outer.put("set", innerSet);

        Map<String, Object> copy = deepCopy(outer, ORDERED);

        @SuppressWarnings("unchecked")
        Set<Object> copiedSet = (Set<Object>) copy.get("set");
        assertNotSame(innerSet, copiedSet);
        assertEquals(List.of("charlie", "alpha", "bravo"), new ArrayList<>(copiedSet));
    }

    public void testDeepCopyUnmodifiableAndOrdered() {
        Map<String, Object> original = new LinkedHashMap<>();
        original.put("b", 2);
        original.put("a", 1);

        Map<String, Object> copy = deepCopy(original, UNMODIFIABLE, ORDERED);
        assertEquals(List.of("b", "a"), new ArrayList<>(copy.keySet()));
        expectThrows(UnsupportedOperationException.class, () -> copy.put("c", 3));
    }

    public void testDeepCopyUnmodifiableAndOrderedSet() {
        Set<Object> original = new LinkedHashSet<>();
        original.add("charlie");
        original.add("alpha");
        original.add("bravo");

        Set<Object> copy = deepCopy(original, ORDERED, UNMODIFIABLE);
        assertEquals(List.of("charlie", "alpha", "bravo"), new ArrayList<>(copy));
        expectThrows(UnsupportedOperationException.class, () -> copy.add("delta"));
    }

    public void testDeepCopyUnmodifiableWithByteArrayThrows() {
        byte[] arr = new byte[] { 1, 2, 3 };
        var e = expectThrows(IllegalArgumentException.class, () -> deepCopy(arr, UNMODIFIABLE));
        assertThat(e.getMessage(), equalTo("cannot make array type [class [B] unmodifiable"));
    }

    public void testDeepCopyUnmodifiableWithDoubleArrayThrows() {
        double[] arr = new double[] { 1.0, 2.0 };
        var e = expectThrows(IllegalArgumentException.class, () -> deepCopy(arr, UNMODIFIABLE));
        assertThat(e.getMessage(), equalTo("cannot make array type [class [D] unmodifiable"));
    }

    public void testDeepCopyUnmodifiableWithDouble2DArrayThrows() {
        double[][] arr = new double[][] { { 1.0 }, { 2.0 } };
        var e = expectThrows(IllegalArgumentException.class, () -> deepCopy(arr, UNMODIFIABLE));
        assertThat(e.getMessage(), equalTo("cannot make array type [class [[D] unmodifiable"));
    }

    public void testDeepCopyUnmodifiableWithNestedByteArrayThrows() {
        // byte[] nested inside a map with UNMODIFIABLE should propagate the exception
        Map<String, Object> map = new HashMap<>();
        map.put("arr", new byte[] { 1, 2 });
        expectThrows(IllegalArgumentException.class, () -> deepCopy(map, UNMODIFIABLE));
    }

    public void testDeepCopyDuplicateOptionsAreHarmless() {
        // UNMODIFIABLE | UNMODIFIABLE is still UNMODIFIABLE
        Map<String, Object> original = new HashMap<>(Map.of("k", "v"));
        Map<String, Object> copy = deepCopy(original, UNMODIFIABLE, UNMODIFIABLE);
        assertEquals(original, copy);
        expectThrows(UnsupportedOperationException.class, () -> copy.put("x", "y"));
    }

    public void testDeepCopyUnknownTypeAssertsInDev() {
        // Tests run with assertions enabled (-ea), so assert false fires as AssertionError.
        // In production (no -ea) the value would be passed through by reference.
        var e = expectThrows(AssertionError.class, () -> deepCopy(new Object()));
        assertThat(e.getMessage(), equalTo("unexpected value type [class java.lang.Object]"));
    }

    public void testDeepCopyUnknownTypeNestedInMapAssertsInDev() {
        Map<String, Object> map = new HashMap<>();
        map.put("bad", new Object());
        expectThrows(AssertionError.class, () -> deepCopy(map));
    }

    public void testDeepCopy() {
        Map<String, Object> original = new HashMap<>();
        {
            original.put("foo", "bar");
            original.put("int", 123);
            original.put("double", 123.0D);
            Map<String, Object> innerObject = new HashMap<>();
            innerObject.put("buzz", "hello world");
            innerObject.put("foo_null", null);
            innerObject.put("1", "bar");
            innerObject.put("long", 123L);
            List<String> innerInnerList = new ArrayList<>();
            innerInnerList.add("item1");
            List<Object> innerList = new ArrayList<>();
            innerList.add(innerInnerList);
            innerObject.put("list", innerList);
            original.put("fizz", innerObject);
            List<Map<String, Object>> list = new ArrayList<>();
            Map<String, Object> value = new HashMap<>();
            value.put("field", "value");
            list.add(value);
            list.add(null);
            original.put("list", list);
            List<String> list2 = new ArrayList<>();
            list2.add("foo");
            list2.add("bar");
            list2.add("baz");
            original.put("list2", list2);
        }

        Map<?, ?> result = deepCopy(original);
        assertThat(result, equalTo(original));
        assertThat(result, not(sameInstance(original)));

        result = deepCopy(original, UNMODIFIABLE);
        assertThat(result, equalTo(original));
        assertThat(result, not(sameInstance(original)));
        Map<?, ?> innerMap = (Map<?, ?>) result.get("fizz");
        expectThrows(UnsupportedOperationException.class, () -> innerMap.remove("x"));
        List<?> innerList = (List<?>) result.get("list");
        expectThrows(UnsupportedOperationException.class, () -> innerList.remove(0));

        original.put("embedded_object", new byte[] { 1, 2, 3 });
        result = deepCopy(original);
        assertArrayEquals(new byte[] { 1, 2, 3 }, (byte[]) result.get("embedded_object"));
    }
}
