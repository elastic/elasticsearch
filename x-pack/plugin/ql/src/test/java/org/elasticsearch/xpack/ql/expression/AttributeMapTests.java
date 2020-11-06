/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.util.stream.Collectors.toList;
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
        Map<Attribute, String> map = new LinkedHashMap<>();
        map.put(a("one"), "one");
        map.put(a("two"), "two");
        map.put(a("three"), "three");

        return new AttributeMap<>(map);
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

        Map<Attribute, Expression> collectRefs = new LinkedHashMap<>();
        for (Alias a : List.of(param1, param2)) {
            collectRefs.put(a.toAttribute(), a.child());
        }
        // we can look up the same item by both attributes
        assertNotNull(collectRefs.get(param1.toAttribute()));
        assertNotNull(collectRefs.get(param2.toAttribute()));
        AttributeMap<Expression> attributeMap = new AttributeMap<>(collectRefs);

        // validate that all Alias can be e
        assertTrue(attributeMap.containsKey(param1.toAttribute()));
        assertFalse(attributeMap.containsKey(param2.toAttribute())); // results in unknown attribute exception

        AttributeMap.Builder<Expression> mapBuilder = AttributeMap.builder();
        for (Alias a : List.of(param1, param2)) {
            mapBuilder.put(a.toAttribute(), a.child());
        }
        AttributeMap<Expression> newAttributeMap = mapBuilder.build();

        assertTrue(newAttributeMap.containsKey(param1.toAttribute()));
        assertTrue(newAttributeMap.containsKey(param2.toAttribute())); // no more unknown attribute exception
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

    public void testMapConstructor() {
        Map<Attribute, String> map = new LinkedHashMap<>();
        map.put(a("one"), "one");
        map.put(a("two"), "two");
        map.put(a("three"), "three");

        AttributeMap<String> m = new AttributeMap<>(map);
        assertThat(m.size(), is(3));
        assertThat(m.isEmpty(), is(false));

        Attribute one = m.keySet().iterator().next();
        assertThat(m.containsKey(one), is(true));
        assertThat(m.containsKey(a("one")), is(false));
        assertThat(m.containsValue("one"), is(true));
        assertThat(m.containsValue("on"), is(false));
        assertThat(m.attributeNames(), contains("one", "two", "three"));
        assertThat(m.values(), contains(map.values().toArray()));

        // defensive copying
        map.put(a("four"), "four");
        assertThat(m.size(), is(3));
        assertThat(m.isEmpty(), is(false));
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

        Map<Attribute, String> map = new LinkedHashMap<>();
        map.put(one, "one");
        map.put(two, "two");
        map.put(three, "three");

        Set<Attribute> keySet = new AttributeMap<>(map).keySet();
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

        Map<Attribute, String> map = new LinkedHashMap<>();
        map.put(one, "one");
        map.put(two, "two");
        map.put(three, "three");

        Set<Entry<Attribute, String>> set = new AttributeMap<>(map).entrySet();

        assertThat(set, hasSize(3));

        List<Attribute> keys = set.stream().map(Map.Entry::getKey).collect(toList());
        List<String> values = set.stream().map(Map.Entry::getValue).collect(toList());

        assertThat(keys, hasSize(3));


        assertThat(values, hasSize(3));
        assertThat(values, contains("one", "two", "three"));
    }

    public void testForEach() {
        AttributeMap<String> m = threeMap();

        Map<Attribute, String> collect = new LinkedHashMap<>();
        m.forEach(collect::put);
        AttributeMap<String> copy = new AttributeMap<>(collect);

        assertThat(m, is(copy));
    }
}
