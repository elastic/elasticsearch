/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.property;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class PropertyPlaceholderTests extends ESTestCase {

    @Test
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

    @Test
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

    @Test
    public void testDefaultValue() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        assertEquals("bar", propertyPlaceholder.replacePlaceholders("${foo:bar}", placeholderResolver));
        assertEquals("", propertyPlaceholder.replacePlaceholders("${foo:}", placeholderResolver));
    }

    @Test
    public void testIgnoredUnresolvedPlaceholder() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", true);
        Map<String, String> map = new LinkedHashMap<>();
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        assertEquals("${foo}", propertyPlaceholder.replacePlaceholders("${foo}", placeholderResolver));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNotIgnoredUnresolvedPlaceholder() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        propertyPlaceholder.replacePlaceholders("${foo}", placeholderResolver);
    }

    @Test
    public void testShouldIgnoreMissing() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, true, true);
        assertEquals("bar", propertyPlaceholder.replacePlaceholders("bar${foo}", placeholderResolver));
    }

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test(expected = IllegalArgumentException.class)
    public void testCircularReference() {
        PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
        Map<String, String> map = new LinkedHashMap<>();
        map.put("foo", "${bar}");
        map.put("bar", "${foo}");
        PropertyPlaceholder.PlaceholderResolver placeholderResolver = new SimplePlaceholderResolver(map, false, true);
        propertyPlaceholder.replacePlaceholders("${foo}", placeholderResolver);
    }

    @Test
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
