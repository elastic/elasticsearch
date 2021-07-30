/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.utils.NameResolver;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class NameResolverTests extends ESTestCase {

    public void testNoMatchingNames() {
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class,
                () -> newUnaliasedResolver().expand("foo", false));
        assertThat(e.getMessage(), equalTo("foo"));
    }

    public void testNoMatchingNames_GivenPatternAndAllowNoMatch() {
        assertThat(newUnaliasedResolver().expand("foo*", true).isEmpty(), is(true));
    }

    public void testNoMatchingNames_GivenPatternAndNotAllowNoMatch() {
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class,
                () -> newUnaliasedResolver().expand("foo*", false));
        assertThat(e.getMessage(), equalTo("foo*"));
    }

    public void testNoMatchingNames_GivenMatchingNameAndNonMatchingPatternAndNotAllowNoMatch() {
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class,
                () ->  newUnaliasedResolver("foo").expand("foo, bar*", false));
        assertThat(e.getMessage(), equalTo("bar*"));
    }

    public void testUnaliased() {
        NameResolver nameResolver = newUnaliasedResolver("foo-1", "foo-2", "bar-1", "bar-2");

        assertThat(nameResolver.expand("foo-1", false), equalTo(newSortedSet("foo-1")));
        assertThat(nameResolver.expand("foo-2", false), equalTo(newSortedSet("foo-2")));
        assertThat(nameResolver.expand("bar-1", false), equalTo(newSortedSet("bar-1")));
        assertThat(nameResolver.expand("bar-2", false), equalTo(newSortedSet("bar-2")));
        assertThat(nameResolver.expand("foo-1,foo-2", false), equalTo(newSortedSet("foo-1", "foo-2")));
        assertThat(nameResolver.expand("foo-*", false), equalTo(newSortedSet("foo-1", "foo-2")));
        assertThat(nameResolver.expand("bar-*", false), equalTo(newSortedSet("bar-1", "bar-2")));
        assertThat(nameResolver.expand("*oo-*", false), equalTo(newSortedSet("foo-1", "foo-2")));
        assertThat(nameResolver.expand("*-1", false), equalTo(newSortedSet("foo-1", "bar-1")));
        assertThat(nameResolver.expand("*-2", false), equalTo(newSortedSet("foo-2", "bar-2")));
        assertThat(nameResolver.expand("*", false), equalTo(newSortedSet("foo-1", "foo-2", "bar-1", "bar-2")));
        assertThat(nameResolver.expand("_all", false), equalTo(newSortedSet("foo-1", "foo-2", "bar-1", "bar-2")));
        assertThat(nameResolver.expand("foo-1,foo-2", false), equalTo(newSortedSet("foo-1", "foo-2")));
        assertThat(nameResolver.expand("foo-1,bar-1", false), equalTo(newSortedSet("bar-1", "foo-1")));
        assertThat(nameResolver.expand("foo-*,bar-1", false), equalTo(newSortedSet("bar-1", "foo-1", "foo-2")));
    }

    public void testAliased() {
        Map<String, List<String>> namesAndAliasesMap = new HashMap<>();
        namesAndAliasesMap.put("foo-1", Collections.singletonList("foo-1"));
        namesAndAliasesMap.put("foo-2", Collections.singletonList("foo-2"));
        namesAndAliasesMap.put("bar-1", Collections.singletonList("bar-1"));
        namesAndAliasesMap.put("bar-2", Collections.singletonList("bar-2"));
        namesAndAliasesMap.put("foo-group", Arrays.asList("foo-1", "foo-2"));
        namesAndAliasesMap.put("bar-group", Arrays.asList("bar-1", "bar-2"));
        NameResolver nameResolver = new TestAliasNameResolver(namesAndAliasesMap);

        // First try same set of assertions as unaliases
        assertThat(nameResolver.expand("foo-1", false), equalTo(newSortedSet("foo-1")));
        assertThat(nameResolver.expand("foo-2", false), equalTo(newSortedSet("foo-2")));
        assertThat(nameResolver.expand("bar-1", false), equalTo(newSortedSet("bar-1")));
        assertThat(nameResolver.expand("bar-2", false), equalTo(newSortedSet("bar-2")));
        assertThat(nameResolver.expand("foo-1,foo-2", false), equalTo(newSortedSet("foo-1", "foo-2")));
        assertThat(nameResolver.expand("foo-*", false), equalTo(newSortedSet("foo-1", "foo-2")));
        assertThat(nameResolver.expand("bar-*", false), equalTo(newSortedSet("bar-1", "bar-2")));
        assertThat(nameResolver.expand("*oo-*", false), equalTo(newSortedSet("foo-1", "foo-2")));
        assertThat(nameResolver.expand("*-1", false), equalTo(newSortedSet("foo-1", "bar-1")));
        assertThat(nameResolver.expand("*-2", false), equalTo(newSortedSet("foo-2", "bar-2")));
        assertThat(nameResolver.expand("*", false), equalTo(newSortedSet("foo-1", "foo-2", "bar-1", "bar-2")));
        assertThat(nameResolver.expand("_all", false), equalTo(newSortedSet("foo-1", "foo-2", "bar-1", "bar-2")));
        assertThat(nameResolver.expand("foo-1,foo-2", false), equalTo(newSortedSet("foo-1", "foo-2")));
        assertThat(nameResolver.expand("foo-1,bar-1", false), equalTo(newSortedSet("bar-1", "foo-1")));
        assertThat(nameResolver.expand("foo-*,bar-1", false), equalTo(newSortedSet("bar-1", "foo-1", "foo-2")));

        // No let's test the aliases
        assertThat(nameResolver.expand("foo-group", false), equalTo(newSortedSet("foo-1", "foo-2")));
        assertThat(nameResolver.expand("bar-group", false), equalTo(newSortedSet("bar-1", "bar-2")));
        assertThat(nameResolver.expand("foo-group,bar-group", false), equalTo(newSortedSet("bar-1", "bar-2", "foo-1", "foo-2")));
        assertThat(nameResolver.expand("foo-group,foo-1", false), equalTo(newSortedSet("foo-1", "foo-2")));
        assertThat(nameResolver.expand("foo-group,bar-1", false), equalTo(newSortedSet("bar-1", "foo-1", "foo-2")));
        assertThat(nameResolver.expand("foo-group,bar-*", false), equalTo(newSortedSet("bar-1", "bar-2", "foo-1", "foo-2")));
    }

    private static NameResolver newUnaliasedResolver(String... names) {
        return NameResolver.newUnaliased(new HashSet<>(Arrays.asList(names)), notFoundExceptionSupplier());
    }

    private static SortedSet<String> newSortedSet(String... names) {
        SortedSet<String> result = new TreeSet<>();
        for (String name : names) {
            result.add(name);
        }
        return result;
    }

    private static Function<String, ResourceNotFoundException> notFoundExceptionSupplier() {
        return s -> new ResourceNotFoundException(s);
    }

    private static class TestAliasNameResolver extends NameResolver {

        private final Map<String, List<String>> lookup;

        TestAliasNameResolver(Map<String, List<String>> lookup) {
            super(notFoundExceptionSupplier());
            this.lookup = lookup;
        }

        @Override
        protected Set<String> keys() {
            return lookup.keySet();
        }

        @Override
        protected Set<String> nameSet() {
            return lookup.values().stream().flatMap(List::stream).collect(Collectors.toSet());
        }

        @Override
        protected List<String> lookup(String key) {
            return lookup.containsKey(key) ? lookup.get(key) : Collections.emptyList();
        }
    }
}
