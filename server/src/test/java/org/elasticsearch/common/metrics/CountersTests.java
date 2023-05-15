/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.metrics;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;

public class CountersTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testCounters() {
        Counters counters = new Counters();
        counters.inc("f", 200);
        counters.inc("foo.bar");
        counters.inc("foo.baz");
        counters.inc("foo.baz");
        assertThat(counters.get("f"), equalTo(200L));
        assertThat(counters.get("foo.bar"), equalTo(1L));
        assertThat(counters.get("foo.baz"), equalTo(2L));
        expectThrows(IllegalArgumentException.class, () -> counters.get("foo"));
        Map<String, Object> map = counters.toMutableNestedMap();
        assertThat(map, hasEntry("f", 200L));
        assertThat(map, hasKey("foo"));
        assertThat(map.get("foo"), instanceOf(Map.class));
        Map<String, Object> fooMap = (Map<String, Object>) map.get("foo");
        assertThat(fooMap, hasEntry("bar", 1L));
        assertThat(fooMap, hasEntry("baz", 2L));
    }

    public void testMerging() {
        Counters counters1 = new Counters();
        counters1.inc("f", 200);
        counters1.inc("foo.bar");
        counters1.inc("foo.baz");
        counters1.inc("foo.baz");
        Counters counters2 = new Counters();
        Counters counters3 = new Counters();
        counters3.inc("foo.bar", 2);
        counters3.inc("bar.foo");
        Counters mergedCounters = Counters.merge(List.of(counters1, counters2, counters3));
        assertThat(mergedCounters.get("f"), equalTo(200L));
        assertThat(mergedCounters.get("foo.bar"), equalTo(3L));
        assertThat(mergedCounters.get("foo.baz"), equalTo(2L));
        assertThat(mergedCounters.get("bar.foo"), equalTo(1L));
    }

    @SuppressWarnings("unchecked")
    public void testNestedMapMutability() {
        Counters counters = new Counters();
        counters.inc("foo.baz");
        Map<String, Object> map = counters.toMutableNestedMap();
        try {
            map.put("test", 1);
            ((Map<String, Object>) map.get("foo")).put("nested-test", 2);
        } catch (Exception e) {
            fail("The map of the counters should be mutable.");
        }
    }

    public void testPathConflictNestedMapConversion() {
        Counters counters = new Counters();
        counters.inc("foo.bar");
        counters.inc("foo");
        counters.inc("foo.baz");
        assertThat(counters.get("foo.bar"), equalTo(1L));
        assertThat(counters.get("foo"), equalTo(1L));

        try {
            counters.toMutableNestedMap();
            fail("Expected an IllegalStateException but got no exception");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("Failed to convert counter 'foo"));
        } catch (Exception e) {
            fail("Expected an IllegalStateException but got " + e.getClass().getName());
        }
    }
}
