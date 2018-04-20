/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.common.stats;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;

public class CountersTests extends ESTestCase {

    public void testCounters() {
        Counters counters = new Counters();
        counters.inc("f", 200);
        counters.inc("foo.bar");
        counters.inc("foo.baz");
        counters.inc("foo.baz");
        Map<String, Object> map = counters.toNestedMap();
        assertThat(map, hasEntry("f", 200L));
        assertThat(map, hasKey("foo"));
        assertThat(map.get("foo"), instanceOf(Map.class));
        Map<String, Object> fooMap = (Map<String, Object>) map.get("foo");
        assertThat(fooMap, hasEntry("bar", 1L));
        assertThat(fooMap, hasEntry("baz", 2L));
    }
}
