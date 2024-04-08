/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;

public class ObjectPathTests extends ESTestCase {
    public void testEval() {
        Map<String, Object> map = singletonMap("key", "value");

        assertThat(randomEval(new String[] { "key" }, map), is((Object) "value"));
        assertThat(randomEval(new String[] { "key1" }, map), nullValue());
    }

    public void testEvalList() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        Map<String, Object> map = singletonMap("key", list);

        int index = randomInt(3);
        assertThat(randomEval(new String[] { "key", String.valueOf(index) }, map), is(list.get(index)));
    }

    public void testEvalArray() {
        int[] array = new int[] { 1, 2, 3, 4 };
        Map<String, Object> map = singletonMap("key", array);

        int index = randomInt(3);
        assertThat(((Number) randomEval(new String[] { "key", String.valueOf(index) }, map)).intValue(), is(array[index]));
    }

    public void testEvalMap() {
        Map<String, Object> map = singletonMap("a", singletonMap("b", "val"));

        assertThat(randomEval(new String[] { "a", "b" }, map), is((Object) "val"));
    }

    public void testEvalMixed() {
        Map<String, Object> map = new HashMap<>();

        Map<String, Object> mapA = new HashMap<>();
        map.put("a", mapA);

        List<Object> listB = new ArrayList<>();
        mapA.put("b", listB);
        List<Object> listB1 = new ArrayList<>();
        listB.add(listB1);

        Map<String, Object> mapB11 = new HashMap<>();
        listB1.add(mapB11);
        mapB11.put("c", "val");

        assertThat(randomEval(new String[0], map), is((Object) map));
        assertThat(randomEval(new String[] { "a", "b", "0", "0", "c" }, map), is((Object) "val"));
        assertThat(randomEval(new String[] { "a", "b", "0", "0", "c", "d" }, map), nullValue());
        assertThat(randomEval(new String[] { "a", "b", "0", "0", "d" }, map), nullValue());
        assertThat(randomEval(new String[] { "a", "b", "c" }, map), nullValue());
    }

    private <T> T randomEval(String[] path, Object object) {
        if (randomBoolean()) {
            return ObjectPath.eval(String.join(".", path), object);
        } else {
            return ObjectPath.eval(path, object);
        }
    }
}
