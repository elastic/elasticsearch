/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class AttributeMapTests extends ESTestCase {

    private static Attribute a(String name) {
        return new UnresolvedAttribute(Source.EMPTY, name);
    }

    private static Map<Attribute, String> threeMap() {
        Map<Attribute, String> map = new LinkedHashMap<>();
        map.put(a("one"), "one");
        map.put(a("two"), "two");
        map.put(a("three"), "three");
        return map;
    }

    private static AttributeMap<String> threeAttributeMap() {
        return AttributeMap.from(threeMap().entrySet(), Entry::getKey, Entry::getValue);
    }

    public void testEmptyConstructor() {
        AttributeMap<Object> m = new AttributeMap<>();
        assertThat(m.size(), is(0));
        assertThat(m.isEmpty(), is(true));
    }

    @Deprecated
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

    public void testIterableConstructor() {
        Attribute one = a("one");
        Attribute anotherOne = a("one");
        Attribute two = a("two");
        List<Attribute> attributes = List.of(one, anotherOne, two);
        AttributeMap<String> attributeMap = AttributeMap.from(attributes, Function.identity(), Attribute::name);

        assertThat(one.semanticEquals(anotherOne), is(false));
        assertThat(one.semanticHash(), is(not(anotherOne.semanticHash())));
        assertThat(attributeMap.size(), is(attributes.size()));
        assertThat(attributeMap.keySet(), contains(one, anotherOne, two));
    }

    public void testStreamConstructor() {
        Attribute one = a("one");
        Attribute anotherOne = a("one");
        Attribute two = a("two");
        List<Attribute> attributes = List.of(one, anotherOne, two);
        AttributeMap<String> attributeMap = AttributeMap.from(attributes.stream(), Function.identity(), Attribute::name);

        assertThat(one.semanticEquals(anotherOne), is(false));
        assertThat(one.semanticHash(), is(not(anotherOne.semanticHash())));
        assertThat(attributeMap.size(), is(attributes.size()));
        assertThat(attributeMap.keySet(), contains(one, anotherOne, two));
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
        AttributeMap<String> m = threeAttributeMap();
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
        AttributeMap<String> m = threeAttributeMap();
        AttributeMap<String> mo = new AttributeMap<>(m.keySet().iterator().next(), "one");
        AttributeMap<String> empty = new AttributeMap<>();

        assertThat(m.intersect(empty), is(empty));
        assertThat(m.intersect(m), is(m));
        assertThat(mo.intersect(m), is(mo));
    }

    public void testSubsetOf() {
        AttributeMap<String> m = threeAttributeMap();
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

        Set<Attribute> keySet = AttributeMap.from(map.entrySet(), Entry::getKey, Entry::getValue).keySet();
        assertThat(keySet, contains(one, two, three));

        // toObject
        Object[] array = keySet.toArray();

        assertThat(array, arrayWithSize(3));
        assertThat(array, arrayContaining(one, two, three));
    }

    public void testValues() {
        AttributeMap<String> m = threeAttributeMap();
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

        Set<Entry<Attribute, String>> set = AttributeMap.from(map.entrySet(), Entry::getKey, Entry::getValue).entrySet();

        assertThat(set, hasSize(3));

        List<Attribute> keys = set.stream().map(Map.Entry::getKey).collect(toList());
        List<String> values = set.stream().map(Map.Entry::getValue).collect(toList());

        assertThat(keys, hasSize(3));


        assertThat(values, hasSize(3));
        assertThat(values, contains("one", "two", "three"));
    }

    public void testForEach() {
        AttributeMap<String> m = threeAttributeMap();

        Map<Attribute, String> collect = new LinkedHashMap<>();
        m.forEach(collect::put);
        AttributeMap<String> copy = AttributeMap.from(collect.entrySet(), Entry::getKey, Entry::getValue);

        assertThat(m, is(copy));
    }
}
