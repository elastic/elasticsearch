/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.RandomAccess;

import static org.elasticsearch.xpack.esql.core.expression.AttributeMapTests.a;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class AttributeListTests extends ESTestCase {

    public void testOf() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        AttributeList list = AttributeList.of(List.of(a1, a2));
        assertThat(list, contains(a1, a2));
        assertThat(AttributeList.of(list), sameInstance(list));
        assertThat(AttributeList.of(List.of()), sameInstance(AttributeList.EMPTY));
        assertThat(list, instanceOf(RandomAccess.class));
    }

    public void testAppendKeepsEarlierViews() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        Attribute a3 = a("3");
        AttributeList first = AttributeList.of(List.of(a1));
        AttributeList second = first.append(List.of(a2));
        AttributeList third = second.append(List.of(a3));

        assertThat(first, contains(a1));
        assertThat(second, contains(a1, a2));
        assertThat(third, contains(a1, a2, a3));
        expectThrows(IndexOutOfBoundsException.class, () -> first.get(1));
        expectThrows(IndexOutOfBoundsException.class, () -> second.get(2));
        assertArrayEquals(new Object[] { a1, a2 }, second.toArray());
    }

    public void testAppendNothing() {
        AttributeList list = AttributeList.of(List.of(a("1")));
        assertThat(list.append(List.of()), sameInstance(list));
    }

    public void testAppendToEmpty() {
        Attribute a1 = a("1");
        AttributeList list = AttributeList.EMPTY.append(List.of(a1));
        assertThat(list, contains(a1));
        assertTrue(AttributeList.EMPTY.isEmpty());
    }

    public void testAppendingTheSameAttributesAgainGivesEqualLists() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        Attribute a3 = a("3");
        AttributeList base = AttributeList.of(List.of(a1));
        AttributeList appended = base.append(List.of(a2));
        appended.append(List.of(a3));

        AttributeList again = base.append(List.of(a2));
        assertEquals(appended, again);
        assertThat(again, contains(a1, a2));
        assertTrue(AttributeSet.of(again).contains(a2));
        assertFalse(AttributeSet.of(again).contains(a3));
    }

    public void testAppendingDifferentAttributesToTheSameListKeepsBothIndependent() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        Attribute other = a("other");
        AttributeList base = AttributeList.of(List.of(a1));
        AttributeList appended = base.append(List.of(a2));
        AttributeList fork = base.append(List.of(other));

        assertThat(appended, contains(a1, a2));
        assertThat(fork, contains(a1, other));
        assertFalse(AttributeSet.of(fork).contains(a2));
        assertFalse(AttributeSet.of(appended).contains(other));
    }

    public void testContainsAttributeOnlySeesOwnSegments() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        AttributeList first = AttributeList.of(List.of(a1));
        AttributeList second = first.append(List.of(a2));

        assertTrue(second.containsAttribute(a2));
        assertTrue(first.containsAttribute(a1));
        assertFalse(first.containsAttribute(a2));
        assertTrue(second.containsAttribute(new Alias(a2.source(), a2.name(), new Literal(Source.EMPTY, 1, DataType.INTEGER), a2.id())));
        assertFalse(second.containsAttribute("not an attribute"));
    }

    public void testGetAndIterateAcrossSegments() {
        List<Attribute> expected = new ArrayList<>();
        AttributeList list = AttributeList.EMPTY;
        for (int segment = 0; segment < 6; segment++) {
            List<Attribute> added = new ArrayList<>();
            for (int i = 0; i < segment + 1; i++) {
                added.add(a("s" + segment + "_" + i));
            }
            expected.addAll(added);
            list = list.append(added);
        }
        assertEquals(expected.size(), list.size());
        for (int i = 0; i < expected.size(); i++) {
            assertSame(expected.get(i), list.get(i));
        }
        expectThrows(IndexOutOfBoundsException.class, () -> AttributeList.of(expected).get(expected.size()));
        expectThrows(IndexOutOfBoundsException.class, () -> AttributeList.EMPTY.get(0));
        assertEquals(expected, new ArrayList<>(list));
        assertArrayEquals(expected.toArray(), list.toArray());
        List<Attribute> visited = new ArrayList<>();
        list.forEach(visited::add);
        assertEquals(expected, visited);
        assertEquals(expected.subList(3, 11), list.subList(3, 11));
    }

    public void testContainsInLargeAndSmallSegments() {
        List<Attribute> large = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            large.add(a("large" + i));
        }
        Attribute small = a("small");
        AttributeList list = AttributeList.of(large).append(List.of(small));
        for (Attribute attribute : large) {
            assertTrue(list.containsAttribute(attribute));
        }
        assertTrue(list.containsAttribute(small));
        assertFalse(list.containsAttribute(a("absent")));
        assertFalse(AttributeList.of(large).containsAttribute(small));
    }

    public void testOfAndAppendRejectDuplicateNamesAndIds() {
        Attribute a1 = a("1");
        Attribute sameName = a("1");
        Attribute sameId = new ReferenceAttribute(Source.EMPTY, null, "other", DataType.INTEGER, Nullability.FALSE, a1.id(), false);
        expectThrows(IllegalArgumentException.class, () -> AttributeList.of(List.of(a1, sameName)));
        expectThrows(IllegalArgumentException.class, () -> AttributeList.of(List.of(a1, sameId)));
        AttributeList list = AttributeList.of(List.of(a1));
        expectThrows(IllegalArgumentException.class, () -> list.append(List.of(sameName)));
        expectThrows(IllegalArgumentException.class, () -> list.append(List.of(sameId)));
        expectThrows(IllegalArgumentException.class, () -> list.append(List.of(a("2"), a("2"))));
    }

    public void testOfRejectsDuplicatesPastLinearScanLimit() {
        List<Attribute> members = new ArrayList<>();
        for (int i = 0; i < between(AttributeSegments.LINEAR_SCAN_LIMIT + 1, 200); i++) {
            members.add(a("member" + i));
        }
        Attribute existing = randomFrom(members);
        Attribute sameName = a(existing.name());
        Attribute sameId = new ReferenceAttribute(Source.EMPTY, null, "other", DataType.INTEGER, Nullability.FALSE, existing.id(), false);
        assertThat(AttributeList.of(members), contains(members.toArray()));
        for (Attribute duplicate : List.of(sameName, sameId)) {
            List<Attribute> withDuplicate = new ArrayList<>(members);
            withDuplicate.add(between(0, withDuplicate.size()), duplicate);
            expectThrows(IllegalArgumentException.class, () -> AttributeList.of(withDuplicate));
        }
    }

    public void testMergeCopiesPlainChild() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        Attribute shadowing = a("1");
        Attribute a3 = a("3");
        assertThat(AttributeList.merge(List.of(a3), List.of(a1, a2)), allOf(instanceOf(AttributeList.class), contains(a1, a2, a3)));
        assertThat(
            AttributeList.merge(List.of(shadowing), List.of(a1, a2)),
            allOf(instanceOf(AttributeList.class), contains(a2, shadowing))
        );
        List<Attribute> notDistinct = AttributeList.merge(List.of(a3), List.of(a1, a("1")));
        assertThat(notDistinct, not(instanceOf(AttributeList.class)));
        assertEquals(3, notDistinct.size());
    }

    public void testContainsName() {
        List<Attribute> large = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            large.add(a("large" + i));
        }
        AttributeList list = AttributeList.of(large).append(List.of(a("small")));
        assertTrue(list.containsName("large7"));
        assertTrue(list.containsName("small"));
        assertFalse(list.containsName("absent"));
        assertFalse(AttributeList.of(large).containsName("small"));
        assertFalse(AttributeList.EMPTY.containsName("small"));
    }

    public void testLargeSegmentLookups() {
        List<Attribute> members = new ArrayList<>();
        for (int i = 0; i < between(500, 3000); i++) {
            members.add(a("member" + i));
        }
        AttributeList list = AttributeList.of(members);
        for (Attribute member : members) {
            assertTrue(list.containsAttribute(member));
            assertTrue(list.containsName(member.name()));
        }
        for (int i = 0; i < 500; i++) {
            assertFalse(list.containsAttribute(a("outsider" + i)));
            assertFalse(list.containsName("outsider" + i));
        }
    }

    public void testMergeSharesChildAndKeepsItIntact() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        AttributeList child = AttributeList.of(List.of(a1));
        List<Attribute> merged = AttributeList.merge(List.of(a2), child);
        assertEquals(List.of(a1, a2), merged);
        assertEquals(List.of(a1), child);
    }

    public void testEqualsArrayList() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        AttributeList list = AttributeList.of(List.of(a1)).append(List.of(a2));
        List<Attribute> arrayList = new ArrayList<>(List.of(a1, a2));
        assertEquals(arrayList, list);
        assertEquals(list, arrayList);
        assertEquals(arrayList.hashCode(), list.hashCode());
    }

    public void testImmutable() {
        AttributeList list = AttributeList.of(List.of(a("1")));
        expectThrows(UnsupportedOperationException.class, () -> list.add(a("2")));
        expectThrows(UnsupportedOperationException.class, () -> list.set(0, a("2")));
        expectThrows(UnsupportedOperationException.class, () -> list.remove(0));
        expectThrows(UnsupportedOperationException.class, list::clear);
    }

    public void testLongChain() {
        int width = between(1, 100);
        int depth = between(1, 100);
        List<Attribute> expected = new ArrayList<>();
        for (int i = 0; i < width; i++) {
            expected.add(a("w" + i));
        }
        List<AttributeList> views = new ArrayList<>();
        AttributeList list = AttributeList.of(expected);
        views.add(list);
        for (int d = 0; d < depth; d++) {
            Attribute added = a("d" + d);
            list = list.append(List.of(added));
            expected.add(added);
            views.add(list);
        }
        assertEquals(expected, list);
        for (int v = 0; v < views.size(); v++) {
            AttributeList view = views.get(v);
            assertEquals(expected.subList(0, width + v), view);
            AttributeSet set = AttributeSet.of(view);
            assertEquals(width + v, set.size());
            assertTrue(set.contains(expected.get(width + v - 1)));
            if (v < depth) {
                assertFalse(set.contains(expected.get(width + v)));
            }
        }
    }
}
