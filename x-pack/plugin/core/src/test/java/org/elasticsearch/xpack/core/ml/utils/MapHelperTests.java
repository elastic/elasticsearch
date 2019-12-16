/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MapHelperTests extends ESTestCase {

    public void testAbsolutePathStringAsKey() {
        String path = "a.b.c.d";
        Map<String, Object> map = Collections.singletonMap(path, 2);
        assertThat(MapHelper.dig(path, map), equalTo(2));
        assertThat(MapHelper.dig(path, Collections.emptyMap()), is(nullValue()));
    }

    public void testSimplePath() {
        String path = "a.b.c.d";
        Map<String, Object> map = Collections.singletonMap("a",
            Collections.singletonMap("b",
                Collections.singletonMap("c",
                    Collections.singletonMap("d", 2))));
        assertThat(MapHelper.dig(path, map), equalTo(2));

        map = Collections.singletonMap("a",
            Collections.singletonMap("b",
                Collections.singletonMap("e", // Not part of path
                    Collections.singletonMap("d", 2))));
        assertThat(MapHelper.dig(path, map), is(nullValue()));
    }

    public void testSimplePathReturningMap() {
        String path = "a.b.c";
        Map<String, Object> map = Collections.singletonMap("a",
            Collections.singletonMap("b",
                Collections.singletonMap("c",
                    Collections.singletonMap("d", 2))));
        assertThat(MapHelper.dig(path, map), equalTo(Collections.singletonMap("d", 2)));
    }

}
