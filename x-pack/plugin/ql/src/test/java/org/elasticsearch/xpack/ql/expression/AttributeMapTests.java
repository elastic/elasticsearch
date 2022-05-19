/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.ql.TestUtils.fieldAttribute;
import static org.elasticsearch.xpack.ql.TestUtils.of;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

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
        Attribute two = fieldAttribute("two", DataTypes.INTEGER);
        Attribute three = fieldAttribute("three", DataTypes.INTEGER);
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
        Attribute a = fieldAttribute("a", DataTypes.INTEGER);
        Attribute b = fieldAttribute("b", DataTypes.INTEGER);
        builder.put(a, a);
        builder.put(b, a);
        AttributeMap<Object> map = builder.build();

        assertEquals(a, map.resolve(a, "default"));
        assertEquals(a, map.resolve(b, "default"));
        assertEquals("default", map.resolve("non-existing-key", "default"));
    }

    public void testResolveMultiHopCycle() {
        AttributeMap.Builder<Object> builder = AttributeMap.builder();
        Attribute a = fieldAttribute("a", DataTypes.INTEGER);
        Attribute b = fieldAttribute("b", DataTypes.INTEGER);
        Attribute c = fieldAttribute("c", DataTypes.INTEGER);
        Attribute d = fieldAttribute("d", DataTypes.INTEGER);
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
        Literal literal = new Literal(source, value, DataTypes.INTEGER);
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
}
