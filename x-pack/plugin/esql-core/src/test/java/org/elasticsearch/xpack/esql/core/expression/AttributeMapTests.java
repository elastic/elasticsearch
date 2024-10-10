/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.esql.core.util.TestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.core.util.TestUtils.of;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AttributeMapTests extends ESTestCase {

    private static Attribute a(String name) {
        return new UnresolvedAttribute(Source.EMPTY, name);
    }

    private static AttributeMap<String> threeMap() {
        AttributeMap.Builder<String> builder = AttributeMap.builder();
        builder.put(a("one"), "one");
        builder.put(a("two"), "two");
        builder.put(a("three"), "three");

        return builder.build();
    }

    public void testAttributeMapWithSameAliasesCanResolveAttributes() {
        Alias param1 = createIntParameterAlias(1, 100);
        Alias param2 = createIntParameterAlias(2, 100);
        assertTrue(param1.equals(param2));
        assertTrue(param1.semanticEquals(param2));
        // equality on literals
        assertTrue(param1.child().equals(param2.child()));
        assertTrue(param1.child().semanticEquals(param2.child()));
        assertTrue(param1.toAttribute().equals(param2.toAttribute()));
        assertFalse(param1.toAttribute().semanticEquals(param2.toAttribute()));

        AttributeMap.Builder<Expression> mapBuilder = AttributeMap.builder();
        for (Alias a : List.of(param1, param2)) {
            mapBuilder.put(a.toAttribute(), a.child());
        }
        AttributeMap<Expression> newAttributeMap = mapBuilder.build();

        assertTrue(newAttributeMap.containsKey(param1.toAttribute()));
        assertTrue(newAttributeMap.get(param1.toAttribute()) == param1.child());
        assertTrue(newAttributeMap.containsKey(param2.toAttribute()));
        assertTrue(newAttributeMap.get(param2.toAttribute()) == param2.child());
    }

    public void testResolve() {
        AttributeMap.Builder<Object> builder = AttributeMap.builder();
        Attribute one = a("one");
        Attribute two = fieldAttribute("two", DataType.INTEGER);
        Attribute three = fieldAttribute("three", DataType.INTEGER);
        Alias threeAlias = new Alias(Source.EMPTY, "three_alias", three);
        Alias threeAliasAlias = new Alias(Source.EMPTY, "three_alias_alias", threeAlias);
        builder.put(one, of("one"));
        builder.put(two, "two");
        builder.put(three, of("three"));
        builder.put(threeAlias.toAttribute(), threeAlias.child());
        builder.put(threeAliasAlias.toAttribute(), threeAliasAlias.child());
        AttributeMap<Object> map = builder.build();

        assertEquals(of("one"), map.resolve(one));
        assertEquals("two", map.resolve(two));
        assertEquals(of("three"), map.resolve(three));
        assertEquals(of("three"), map.resolve(threeAlias));
        assertEquals(of("three"), map.resolve(threeAliasAlias));
        assertEquals(of("three"), map.resolve(threeAliasAlias, threeAlias));
        Attribute four = a("four");
        assertEquals("not found", map.resolve(four, "not found"));
        assertNull(map.resolve(four));
        assertEquals(four, map.resolve(four, four));
    }

    public void testResolveOneHopCycle() {
        AttributeMap.Builder<Object> builder = AttributeMap.builder();
        Attribute a = fieldAttribute("a", DataType.INTEGER);
        Attribute b = fieldAttribute("b", DataType.INTEGER);
        builder.put(a, a);
        builder.put(b, a);
        AttributeMap<Object> map = builder.build();

        assertEquals(a, map.resolve(a, "default"));
        assertEquals(a, map.resolve(b, "default"));
        assertEquals("default", map.resolve("non-existing-key", "default"));
    }

    public void testResolveMultiHopCycle() {
        AttributeMap.Builder<Object> builder = AttributeMap.builder();
        Attribute a = fieldAttribute("a", DataType.INTEGER);
        Attribute b = fieldAttribute("b", DataType.INTEGER);
        Attribute c = fieldAttribute("c", DataType.INTEGER);
        Attribute d = fieldAttribute("d", DataType.INTEGER);
        builder.put(a, b);
        builder.put(b, c);
        builder.put(c, d);
        builder.put(d, a);
        AttributeMap<Object> map = builder.build();

        // note: multi hop cycles should not happen, unless we have a
        // bug in the code that populates the AttributeMaps
        expectThrows(QlIllegalArgumentException.class, () -> { assertEquals(a, map.resolve(a, c)); });
    }

    private Alias createIntParameterAlias(int index, int value) {
        Source source = new Source(1, index * 5, "?");
        Literal literal = new Literal(source, value, DataType.INTEGER);
        Alias alias = new Alias(literal.source(), literal.source().text(), literal);
        return alias;
    }

    public void testEmptyConstructor() {
        AttributeMap<Object> m = new AttributeMap<>();
        assertThat(m.size(), is(0));
        assertThat(m.isEmpty(), is(true));
    }

    public void testBuilder() {
        AttributeMap.Builder<String> builder = AttributeMap.builder();
        builder.put(a("one"), "one");
        builder.put(a("two"), "two");
        builder.put(a("three"), "three");

        AttributeMap<String> m = builder.build();
        assertThat(m.size(), is(3));
        assertThat(m.isEmpty(), is(false));

        Attribute one = m.keySet().iterator().next();
        assertThat(m.containsKey(one), is(true));
        assertThat(m.containsKey(a("one")), is(false));
        assertThat(m.containsValue("one"), is(true));
        assertThat(m.containsValue("on"), is(false));
        assertThat(m.attributeNames(), contains("one", "two", "three"));
        assertThat(m.values(), contains("one", "two", "three"));
    }

    public void testSingleItemConstructor() {
        Attribute one = a("one");
        AttributeMap<String> m = new AttributeMap<>(one, "one");
        assertThat(m.size(), is(1));
        assertThat(m.isEmpty(), is(false));

        assertThat(m.containsKey(one), is(true));
        assertThat(m.containsKey(a("one")), is(false));
        assertThat(m.containsValue("one"), is(true));
        assertThat(m.containsValue("on"), is(false));
    }

    public void testSubtract() {
        AttributeMap<String> m = threeMap();
        AttributeMap<String> mo = new AttributeMap<>(m.keySet().iterator().next(), "one");
        AttributeMap<String> empty = new AttributeMap<>();

        assertThat(m.subtract(empty), is(m));
        assertThat(m.subtract(m), is(empty));
        assertThat(mo.subtract(m), is(empty));

        AttributeMap<String> subtract = m.subtract(mo);

        assertThat(subtract.size(), is(2));
        assertThat(subtract.attributeNames(), contains("two", "three"));
    }

    public void testIntersect() {
        AttributeMap<String> m = threeMap();
        AttributeMap<String> mo = new AttributeMap<>(m.keySet().iterator().next(), "one");
        AttributeMap<String> empty = new AttributeMap<>();

        assertThat(m.intersect(empty), is(empty));
        assertThat(m.intersect(m), is(m));
        assertThat(mo.intersect(m), is(mo));
    }

    public void testSubsetOf() {
        AttributeMap<String> m = threeMap();
        AttributeMap<String> mo = new AttributeMap<>(m.keySet().iterator().next(), "one");
        AttributeMap<String> empty = new AttributeMap<>();

        assertThat(m.subsetOf(empty), is(false));
        assertThat(m.subsetOf(m), is(true));
        assertThat(mo.subsetOf(m), is(true));

        assertThat(empty.subsetOf(m), is(true));
        assertThat(mo.subsetOf(m), is(true));
    }

    public void testKeySet() {
        Attribute one = a("one");
        Attribute two = a("two");
        Attribute three = a("three");

        Set<Attribute> keySet = threeMap().keySet();
        assertThat(keySet, contains(one, two, three));

        // toObject
        Object[] array = keySet.toArray();

        assertThat(array, arrayWithSize(3));
        assertThat(array, arrayContaining(one, two, three));
    }

    public void testValues() {
        AttributeMap<String> m = threeMap();
        Collection<String> values = m.values();

        assertThat(values, hasSize(3));
        assertThat(values, contains("one", "two", "three"));
    }

    public void testEntrySet() {
        Attribute one = a("one");
        Attribute two = a("two");
        Attribute three = a("three");

        Set<Entry<Attribute, String>> set = threeMap().entrySet();

        assertThat(set, hasSize(3));

        List<Attribute> keys = set.stream().map(Map.Entry::getKey).collect(toList());
        List<String> values = set.stream().map(Map.Entry::getValue).collect(toList());

        assertThat(keys, hasSize(3));

        assertThat(values, hasSize(3));
        assertThat(values, contains("one", "two", "three"));
    }

    public void testCopy() {
        AttributeMap<String> m = threeMap();
        AttributeMap<String> copy = AttributeMap.<String>builder().putAll(m).build();

        assertThat(m, is(copy));
    }

    public void testEmptyMapIsImmutable() {
        var empty = AttributeMap.emptyAttributeMap();
        var ex = expectThrows(UnsupportedOperationException.class, () -> empty.add(a("one"), new Object()));
    }

    public void testAddPutEntriesIntoMap() {
        var map = new AttributeMap<String>();
        var one = a("one");
        var two = a("two");
        var three = a("three");

        for (var i : asList(one, two, three)) {
            map.add(i, i.name());
        }

        assertThat(map.size(), is(3));

        assertThat(map.remove(one), is("one"));
        assertThat(map.remove(two), is("two"));

        assertThat(map.size(), is(1));
    }

    public void testKeyIteratorRemoval() {
        var map = new AttributeMap<String>();
        var one = a("one");
        var two = a("two");
        var three = a("three");

        for (var i : asList(one, two, three)) {
            map.add(i, i.name());
        }

        assertThat(map.attributeNames(), contains("one", "two", "three"));
        assertThat(map.size(), is(3));

        var it = map.keySet().iterator();
        var next = it.next();
        assertThat(next, sameInstance(one));
        it.remove();
        assertThat(map.size(), is(2));
        next = it.next();
        assertThat(next, sameInstance(two));
        next = it.next();

        assertThat(next, sameInstance(three));
        it.remove();
        assertThat(map.size(), is(1));

        assertThat(it.hasNext(), is(false));
    }

    public void testValuesIteratorRemoval() {
        var map = new AttributeMap<String>();
        var one = a("one");
        var two = a("two");
        var three = a("three");

        for (var i : asList(one, two, three)) {
            map.add(i, i.name());
        }

        assertThat(map.values(), contains("one", "two", "three"));

        map.values().removeIf(v -> v.contains("o"));
        assertThat(map.size(), is(1));
        assertThat(map.containsKey(three), is(true));
        assertThat(map.containsValue("three"), is(true));

        assertThat(map.containsKey("two"), is(false));
        assertThat(map.containsKey(one), is(false));

        var it = map.values().iterator();
        assertThat(it.hasNext(), is(true));
        assertThat(it.next(), is("three"));
        it.remove();
        assertThat(it.hasNext(), is(false));
    }
}
