/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import java.util.LinkedHashMap;
import java.util.Map;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class PropertyPlaceholderTests extends ESTestCase {
    public void testSimple() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("{", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        map.put("foo1", "bar1");
        map.put("foo2", "bar2");
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        assertEquals("bar1", propertyPlaceholder.replacePlaceholders("{foo1}", placeholderResolver));
        assertEquals("a bar1b", propertyPlaceholder.replacePlaceholders("a {foo1}b", placeholderResolver));
        assertEquals("bar1bar2", propertyPlaceholder.replacePlaceholders("{foo1}{foo2}", placeholderResolver));
        assertEquals("a bar1 b bar2 c", propertyPlaceholder.replacePlaceholders("a {foo1} b {foo2} c", placeholderResolver));
    }

    public void testVariousPrefixSuffix() {
        // Test various prefix/suffix lengths
        PropertyPlaceholder ppEqualsPrefix = new PropertyPlaceholder("{", "}", false);
        PropertyPlaceholder ppLongerPrefix = new PropertyPlaceholder("${", "}", false);
        PropertyPlaceholder ppShorterPrefix = new PropertyPlaceholder("{", "}}", false);
        Map<String, String> map = new LinkedHashMap<>();
        map.put("foo", "bar");
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        assertEquals("bar", ppEqualsPrefix.replacePlaceholders("{foo}", placeholderResolver));
        assertEquals("bar", ppLongerPrefix.replacePlaceholders("${foo}", placeholderResolver));
        assertEquals("bar", ppShorterPrefix.replacePlaceholders("{foo}}", placeholderResolver));
    }

    public void testDefaultValue() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        assertEquals("bar", propertyPlaceholder.replacePlaceholders("${foo:bar}", placeholderResolver));
        assertEquals("", propertyPlaceholder.replacePlaceholders("${foo:}", placeholderResolver));
    }

    public void testIgnoredUnresolvedPlaceholder() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", true);
        Map<String, String> map = new LinkedHashMap<>();
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        assertEquals("${foo}", propertyPlaceholder.replacePlaceholders("${foo}", placeholderResolver));
    }

    public void testNotIgnoredUnresolvedPlaceholder() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        try {
            propertyPlaceholder.replacePlaceholders("${foo}", placeholderResolver);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("Could not resolve placeholder 'foo'"));
        }
    }

    public void testShouldIgnoreMissing() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, true, true);
        assertEquals("bar", propertyPlaceholder.replacePlaceholders("bar${foo}", placeholderResolver));
    }

    public void testRecursive() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        map.put("foo", "${foo1}");
        map.put("foo1", "${foo2}");
        map.put("foo2", "bar");
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        assertEquals("bar", propertyPlaceholder.replacePlaceholders("${foo}", placeholderResolver));
        assertEquals("abarb", propertyPlaceholder.replacePlaceholders("a${foo}b", placeholderResolver));
    }

    public void testNestedLongerPrefix() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        map.put("foo", "${foo1}");
        map.put("foo1", "${foo2}");
        map.put("foo2", "bar");
        map.put("barbar", "baz");
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        assertEquals("baz", propertyPlaceholder.replacePlaceholders("${bar${foo}}", placeholderResolver));
    }

    public void testNestedSameLengthPrefixSuffix() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("{", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        map.put("foo", "{foo1}");
        map.put("foo1", "{foo2}");
        map.put("foo2", "bar");
        map.put("barbar", "baz");
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        assertEquals("baz", propertyPlaceholder.replacePlaceholders("{bar{foo}}", placeholderResolver));
    }

    public void testNestedShorterPrefix() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("{", "}}", false);
        Map<String, String> map = new LinkedHashMap<>();
        map.put("foo", "{foo1}}");
        map.put("foo1", "{foo2}}");
        map.put("foo2", "bar");
        map.put("barbar", "baz");
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        assertEquals("baz", propertyPlaceholder.replacePlaceholders("{bar{foo}}}}", placeholderResolver));
    }

    public void testCircularReference() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        map.put("foo", "${bar}");
        map.put("bar", "${foo}");
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        try {
            propertyPlaceholder.replacePlaceholders("${foo}", placeholderResolver);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("Circular placeholder reference 'foo' in property definitions"));
        }
    }

    public void testShouldRemoveMissing() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, true, false);
        assertEquals("bar${foo}", propertyPlaceholder.replacePlaceholders("bar${foo}", placeholderResolver));
    }

    private class SimplePlaceholderResolver implements PropertyPlaceholder.PlaceholderResolver {
        private Map<String, String> map;
        private boolean shouldIgnoreMissing;
        private boolean shouldRemoveMissing;

        SimplePlaceholderResolver(Map<String, String> map, boolean shouldIgnoreMissing, boolean shouldRemoveMissing) {
            this.map = map;
            this.shouldIgnoreMissing = shouldIgnoreMissing;
            this.shouldRemoveMissing = shouldRemoveMissing;
        }

        @Override
        public String resolvePlaceholder(String placeholderName) {
            return map.get(placeholderName);
        }

        @Override
        public boolean shouldIgnoreMissing(String placeholderName) {
            return shouldIgnoreMissing;
        }

        @Override
        public boolean shouldRemoveMissingPlaceholder(String placeholderName) {
            return shouldRemoveMissing;
        }
    }
}
