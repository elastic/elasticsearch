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

package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
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
        assertEquals(e.getMessage(), "Unstashing has caused a key conflict! The map is [{foobar=whatever}] and the key is ["
                            + key + "] which unstashes to [foobar]");
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
        stash.stashValue("body", singletonMap("foo", Arrays.asList("a", "b")));

        Map<String, Object> expected;
        Map<String, Object> map;
        if (randomBoolean()) {
            expected = singletonMap("foo", Arrays.asList("test", "boooooh!"));
            map = singletonMap("foo", Arrays.asList("test", "${body.$_path}oooooh!"));
        } else {
            expected = singletonMap("foo", Arrays.asList("test", "b"));
            map = singletonMap("foo", Arrays.asList("test", "$body.$_path"));
        }

        Map<String, Object> actual = stash.replaceStashedValues(map);
        assertEquals(expected, actual);
        assertThat(actual, not(sameInstance(map)));
    }

    public void testPathInMapValue() throws IOException {
        Stash stash = new Stash();
        stash.stashValue("body", singletonMap("foo", singletonMap("a", "b")));

        Map<String, Object> expected;
        Map<String, Object> map;
        if (randomBoolean()) {
            expected = singletonMap("foo", singletonMap("a", "boooooh!"));
            map = singletonMap("foo", singletonMap("a", "${body.$_path}oooooh!"));
        } else {
            expected = singletonMap("foo", singletonMap("a", "b"));
            map = singletonMap("foo", singletonMap("a", "$body.$_path"));
        }

        Map<String, Object> actual = stash.replaceStashedValues(map);
        assertEquals(expected, actual);
        assertThat(actual, not(sameInstance(map)));
    }

}
