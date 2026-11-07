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
import java.util.LinkedHashSet;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.AttributeMapTests.a;

public class AttributeSetTests extends ESTestCase {

    private static final boolean SHARED = AttributeList.SHARED_OUTPUT_FEATURE_FLAG.isEnabled();

    public void testEquals() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");

        AttributeSet first = AttributeSet.of(a1, a2);
        assertEquals(first, first);

        var secondBuilder = AttributeSet.builder();
        secondBuilder.add(a1);
        secondBuilder.add(a2);

        var second = secondBuilder.build();
        assertEquals(first, second);
        assertEquals(second, first);

        var thirdBuilder = AttributeSet.builder();
        thirdBuilder.add(a("1"));
        thirdBuilder.add(a("2"));

        AttributeSet third = thirdBuilder.build();
        assertNotEquals(first, third);
        assertNotEquals(third, first);

        assertEquals(AttributeSet.EMPTY, AttributeSet.EMPTY);
        assertEquals(AttributeSet.EMPTY, first.intersect(third));
        assertEquals(third.intersect(first), AttributeSet.EMPTY);

        assertNotEquals(AttributeSet.EMPTY, AttributeMap.emptyAttributeMap());
        assertNotEquals(AttributeMap.emptyAttributeMap(), AttributeSet.EMPTY);
    }

    public void testSetOfListEqualsSetOfAttributes() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        AttributeSet ofAttributes = AttributeSet.of(a1, a2);
        AttributeSet ofList = AttributeSet.of(AttributeList.of(List.of(a2)).append(List.of(a1)));

        assertEquals(ofAttributes, ofList);
        assertEquals(ofList, ofAttributes);
        assertEquals(ofAttributes.hashCode(), ofList.hashCode());
        assertNotEquals(AttributeSet.of(AttributeList.of(List.of(a1))), ofAttributes);
        assertNotEquals(ofAttributes, AttributeSet.of(AttributeList.of(List.of(a1))));
    }

    public void testSetOfList() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        Attribute a3 = a("3");
        AttributeList shorter = AttributeList.of(List.of(a1, a2));
        AttributeList longer = shorter.append(List.of(a3));
        AttributeSet set = AttributeSet.of(shorter);

        assertEquals(2, set.size());
        assertFalse(set.isEmpty());
        assertTrue(set.contains(a1));
        assertFalse(set.contains(a3));
        assertTrue(AttributeSet.of(longer).contains(a3));
        assertTrue(set.containsAll(List.of(a1, a2)));
        assertFalse(set.containsAll(List.of(a1, a3)));
        assertEquals(new LinkedHashSet<>(List.of("1", "2")), set.names());
        assertEquals(List.of(a1, a2), set.stream().toList());
        List<Attribute> visited = new ArrayList<>();
        set.forEach(visited::add);
        assertEquals(List.of(a1, a2), visited);
        assertEquals(List.of(a1, a2).toString(), set.toString());
        expectThrows(UnsupportedOperationException.class, () -> set.add(a3));
        if (SHARED) {
            expectThrows(UnsupportedOperationException.class, () -> {
                var it = set.iterator();
                it.next();
                it.remove();
            });
        }
    }

    public void testContainsName() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        AttributeSet ofAttributes = AttributeSet.of(a1, a2);
        AttributeSet ofList = AttributeSet.of(AttributeList.of(List.of(a1, a2)));
        for (AttributeSet set : List.of(ofAttributes, ofList)) {
            assertTrue(set.containsName("1"));
            assertTrue(set.containsName("2"));
            assertFalse(set.containsName("3"));
        }
        assertFalse(AttributeSet.EMPTY.containsName("1"));
    }

    public void testAlgebraAcrossSetsOfListsAndAttributes() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        Attribute a3 = a("3");
        AttributeSet ofList = AttributeSet.of(AttributeList.of(List.of(a1, a2)));
        AttributeSet ofAttributes = AttributeSet.of(a2, a3);

        assertEquals(AttributeSet.of(a1, a2, a3), ofList.combine(ofAttributes));
        assertEquals(AttributeSet.of(a1, a2, a3), ofAttributes.combine(ofList));
        assertEquals(AttributeSet.of(a1), ofList.subtract(ofAttributes));
        assertEquals(AttributeSet.of(a3), ofAttributes.subtract(ofList));
        assertEquals(AttributeSet.of(a2), ofList.intersect(ofAttributes));
        assertEquals(AttributeSet.of(a2), ofAttributes.intersect(ofList));
        assertTrue(AttributeSet.of(a2).subsetOf(ofList));
        assertTrue(AttributeSet.of(AttributeList.of(List.of(a2))).subsetOf(ofAttributes));
        assertFalse(ofList.subsetOf(ofAttributes));
        assertEquals(AttributeSet.of(a1, a2), ofList.asBuilder().build());
        assertEquals(AttributeSet.of(a1, a2, a3), AttributeSet.builder().addAll(ofList).add(a3).build());
    }

    public void testAlgebraKeepsOrder() {
        Attribute a1 = a("1");
        Attribute a2 = a("2");
        Attribute a3 = a("3");
        Attribute a4 = a("4");
        AttributeSet first = AttributeSet.of(a1, a2, a3);
        AttributeSet second = AttributeSet.of(a4, a2);

        assertEquals(List.of(a1, a2, a3, a4), List.copyOf(first.combine(second)));
        assertEquals(List.of(a1, a3), List.copyOf(first.subtract(second)));
        assertEquals(List.of(a2), List.copyOf(first.intersect(second)));
        if (SHARED) {
            assertSame(first, first.combine(AttributeSet.of(a2)));
            assertSame(first, first.subtract(AttributeSet.of(a4)));
            AttributeSet contained = AttributeSet.of(a2);
            assertSame(contained, first.intersect(contained));
        }
    }

    public void testBuilderKeepsFirstOfEachId() {
        Attribute a1 = a("1");
        Attribute sameId = new ReferenceAttribute(Source.EMPTY, null, "renamed", DataType.INTEGER, Nullability.TRUE, a1.id(), false);
        AttributeSet.Builder builder = AttributeSet.builder().add(a1).add(sameId).add(a("2"));
        AttributeSet set = builder.build();
        assertEquals(2, set.size());
        assertSame(a1, set.iterator().next());
        assertTrue(set.contains(sameId));

        if (SHARED) {
            builder.add(a("3"));
            builder.removeIf(a -> a.name().equals("1"));
            assertEquals(2, set.size());
            assertTrue(set.contains(a1));
        }
    }

    public void testBuilderAcrossLinearScanLimit() {
        List<Attribute> members = new ArrayList<>();
        int count = AttributeSegments.LINEAR_SCAN_LIMIT + between(-3, 5);
        for (int i = 0; i < count; i++) {
            members.add(a("m" + i));
        }
        AttributeSet.Builder builder = AttributeSet.builder();
        for (Attribute member : members) {
            Attribute sameId = new ReferenceAttribute(Source.EMPTY, null, "x", DataType.INTEGER, Nullability.TRUE, member.id(), false);
            builder.add(member).add(sameId);
        }
        assertEquals(members, new ArrayList<>(builder.build()));
        members.forEach(m -> assertTrue(builder.contains(m)));
        assertFalse(builder.contains(a("absent")));

        Attribute first = members.get(0);
        assertTrue(builder.remove(first));
        assertFalse(builder.remove(first));
        assertFalse(builder.contains(first));
        assertTrue(builder.removeIf(m -> m.name().equals("m1")));
        assertFalse(builder.removeIf(m -> m.name().equals("m1")));
        assertEquals(members.subList(2, members.size()), new ArrayList<>(builder.build()));

        AttributeSet.Builder copy = AttributeSet.builder().addAll(builder);
        assertEquals(builder.build(), copy.build());
        builder.clear();
        assertTrue(builder.isEmpty());
        assertEquals(AttributeSet.EMPTY, builder.build());
        if (SHARED) {
            assertSame(AttributeSet.EMPTY, builder.build());
        }
        assertFalse(copy.isEmpty());
    }

    public void testOfSingle() {
        Attribute a1 = a("1");
        Attribute[] attrs = { a1 };
        AttributeSet set = AttributeSet.of(attrs);
        attrs[0] = a("2");
        assertEquals(List.of(a1), new ArrayList<>(set));
        assertEquals(AttributeSet.EMPTY, AttributeSet.of());
        if (SHARED) {
            assertSame(AttributeSet.EMPTY, AttributeSet.of());
        }
    }

    public void testSameNameDifferentIds() {
        Attribute left = a("k");
        Attribute right = a("k");
        AttributeSet set = AttributeSet.of(left, right);
        assertEquals(2, set.size());
        assertTrue(set.contains(left));
        assertTrue(set.contains(right));
        assertTrue(set.containsName("k"));
        assertEquals(new LinkedHashSet<>(List.of("k")), set.names());
    }

    public void testLargeSets() {
        List<Attribute> members = new ArrayList<>();
        int count = between(9, 2000);
        for (int i = 0; i < count; i++) {
            members.add(a("m" + i));
        }
        AttributeSet set = AttributeSet.of(members);
        AttributeSet combined = set.combine(AttributeSet.of(a("extra")));
        for (Attribute member : members) {
            assertTrue(set.contains(member));
            assertTrue(combined.contains(member));
            assertTrue(set.containsName(member.name()));
        }
        assertFalse(set.contains(a("absent")));
        assertFalse(set.containsName("extra"));
        assertTrue(combined.containsName("extra"));
        assertEquals(members.size() + 1, combined.size());
        assertEquals(set, AttributeSet.of(AttributeList.of(members)));
        assertEquals(set.hashCode(), AttributeSet.of(AttributeList.of(members)).hashCode());
    }

    public void testSetIsImmutable() {
        AttributeSet set = AttributeSet.of(a("1"), a("2"));
        expectThrows(UnsupportedOperationException.class, () -> set.add(a("3")));
        expectThrows(UnsupportedOperationException.class, () -> set.remove(a("1")));
        expectThrows(UnsupportedOperationException.class, () -> set.addAll(set));
        expectThrows(UnsupportedOperationException.class, () -> set.retainAll(set));
        expectThrows(UnsupportedOperationException.class, () -> set.removeAll(set));
        expectThrows(UnsupportedOperationException.class, set::clear);
        expectThrows(UnsupportedOperationException.class, () -> set.removeIf((x -> true)));
    }
}
