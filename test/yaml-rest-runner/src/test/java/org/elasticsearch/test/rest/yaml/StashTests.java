/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.Stash;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class StashTests extends ESTestCase {
    public void testReplaceStashedValuesStashKeyInMapValue() throws IOException {
        Stash stash = new Stash();

        Map<String, Object> expected = new HashMap<>();
        expected.put("key", singletonMap("a", "foobar"));
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> map2 = new HashMap<>();
        if (randomBoolean()) {
            stash.stashValue("stashed", "bar");
            map2.put("a", "foo${stashed}");
        } else {
            stash.stashValue("stashed", "foobar");
            map2.put("a", "$stashed");
        }
        map.put("key", map2);

        Map<String, Object> actual = stash.replaceStashedValues(map);
        assertEquals(expected, actual);
        assertThat(actual, not(sameInstance(map)));
    }

    public void testReplaceStashedValuesStashKeyInMapKey() throws IOException {
        Stash stash = new Stash();

        Map<String, Object> expected = new HashMap<>();
        expected.put("key", singletonMap("foobar", "a"));
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> map2 = new HashMap<>();
        if (randomBoolean()) {
            stash.stashValue("stashed", "bar");
            map2.put("foo${stashed}", "a");
        } else {
            stash.stashValue("stashed", "foobar");
            map2.put("$stashed", "a");
        }
        map.put("key", map2);

        Map<String, Object> actual = stash.replaceStashedValues(map);
        assertEquals(expected, actual);
        assertThat(actual, not(sameInstance(map)));
    }

    public void testReplaceStashedValuesStashKeyInMapKeyConflicts() throws IOException {
        Stash stash = new Stash();

        Map<String, Object> map = new HashMap<>();
        Map<String, Object> map2 = new HashMap<>();
        String key;
        if (randomBoolean()) {
            stash.stashValue("stashed", "bar");
            key = "foo${stashed}";
        } else {
            stash.stashValue("stashed", "foobar");
            key = "$stashed";
        }
        map2.put(key, "a");
        map2.put("foobar", "whatever");
        map.put("key", map2);

        Exception e = expectThrows(IllegalArgumentException.class, () -> stash.replaceStashedValues(map));
        assertEquals(
            e.getMessage(),
            "Unstashing has caused a key conflict! The map is [{foobar=whatever}] and the key is [" + key + "] which unstashes to [foobar]"
        );
    }

    public void testReplaceStashedValuesStashKeyInList() throws IOException {
        Stash stash = new Stash();
        stash.stashValue("stashed", "bar");

        Map<String, Object> expected = new HashMap<>();
        expected.put("key", Arrays.asList("foot", "foobar", 1));
        Map<String, Object> map = new HashMap<>();
        Object value;
        if (randomBoolean()) {
            stash.stashValue("stashed", "bar");
            value = "foo${stashed}";
        } else {
            stash.stashValue("stashed", "foobar");
            value = "$stashed";
        }
        map.put("key", Arrays.asList("foot", value, 1));

        Map<String, Object> actual = stash.replaceStashedValues(map);
        assertEquals(expected, actual);
        assertThat(actual, not(sameInstance(map)));
    }

    public void testPathInList() throws IOException {
        Stash stash = new Stash();
        String topLevelKey;
        if (randomBoolean()) {
            topLevelKey = randomAlphaOfLength(2) + "." + randomAlphaOfLength(2);
        } else {
            topLevelKey = randomAlphaOfLength(5);
        }
        stash.stashValue("body", singletonMap(topLevelKey, Arrays.asList("a", "b")));

        Map<String, Object> expected;
        Map<String, Object> map;
        if (randomBoolean()) {
            expected = singletonMap(topLevelKey, Arrays.asList("test", "boooooh!"));
            map = singletonMap(topLevelKey, Arrays.asList("test", "${body.$_path}oooooh!"));
        } else {
            expected = singletonMap(topLevelKey, Arrays.asList("test", "b"));
            map = singletonMap(topLevelKey, Arrays.asList("test", "$body.$_path"));
        }

        Map<String, Object> actual = stash.replaceStashedValues(map);
        assertEquals(expected, actual);
        assertThat(actual, not(sameInstance(map)));
    }

    public void testPathInMapValue() throws IOException {
        Stash stash = new Stash();
        String topLevelKey;
        if (randomBoolean()) {
            topLevelKey = randomAlphaOfLength(2) + "." + randomAlphaOfLength(2);
        } else {
            topLevelKey = randomAlphaOfLength(5);
        }
        stash.stashValue("body", singletonMap(topLevelKey, singletonMap("a", "b")));

        Map<String, Object> expected;
        Map<String, Object> map;
        if (randomBoolean()) {
            expected = singletonMap(topLevelKey, singletonMap("a", "boooooh!"));
            map = singletonMap(topLevelKey, singletonMap("a", "${body.$_path}oooooh!"));
        } else {
            expected = singletonMap(topLevelKey, singletonMap("a", "b"));
            map = singletonMap(topLevelKey, singletonMap("a", "$body.$_path"));
        }

        Map<String, Object> actual = stash.replaceStashedValues(map);
        assertEquals(expected, actual);
        assertThat(actual, not(sameInstance(map)));
    }

    public void testEscapeExtendedKey() throws IOException {
        Stash stash = new Stash();

        Map<String, Object> map = new HashMap<>();
        map.put("key", singletonMap("a", "foo\\${bar}"));

        Map<String, Object> actual = stash.replaceStashedValues(map);
        assertMap(actual, matchesMap().entry("key", matchesMap().entry("a", "foo${bar}")));
        assertThat(actual, not(sameInstance(map)));
    }

    public void testMultipleVariableNamesInPath() throws Exception {
        var stash = new Stash();
        stash.stashValue("body", Map.of(".ds-k8s-2021-12-15-1", Map.of("data_stream", "k8s", "settings", Map.of(), "mappings", Map.of())));
        stash.stashValue("backing_index", ".ds-k8s-2021-12-15-1");
        assertThat(stash.getValue("$body.$backing_index.data_stream"), equalTo("k8s"));
        assertThat(stash.getValue("$body.$backing_index.settings"), equalTo(Map.of()));
        assertThat(stash.getValue("$body.$backing_index.mappings"), equalTo(Map.of()));
    }

}
