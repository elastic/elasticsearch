/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.xcontent;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class MapPathTests extends ESTestCase {

    @Test
    public void testEval() throws Exception {
        Map<String, Object> map = ImmutableMap.<String, Object>builder()
                .put("key", "value")
                .build();

        assertThat(ObjectPath.eval("key", map), is((Object) "value"));
        assertThat(ObjectPath.eval("key1", map), nullValue());
    }

    @Test
    public void testEval_List() throws Exception {
        List list = Arrays.asList(1, 2, 3, 4);
        Map<String, Object> map = ImmutableMap.<String, Object>builder()
                .put("key", list)
                .build();

        int index = randomInt(3);
        assertThat(ObjectPath.eval("key." + index, map), is(list.get(index)));
    }

    @Test
    public void testEval_Array() throws Exception {
        int[] array = new int[] { 1, 2, 3, 4 };
        Map<String, Object> map = ImmutableMap.<String, Object>builder()
                .put("key", array)
                .build();

        int index = randomInt(3);
        assertThat(((Number) ObjectPath.eval("key." + index, map)).intValue(), is(array[index]));
    }

    @Test
    public void testEval_Map() throws Exception {
        Map<String, Object> map = ImmutableMap.<String, Object>builder()
                .put("a", ImmutableMap.of("b", "val"))
                .build();

        assertThat(ObjectPath.eval("a.b", map), is((Object) "val"));
    }


    @Test
    public void testEval_Mixed() throws Exception {
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

        assertThat(ObjectPath.eval("", map), is((Object) map));
        assertThat(ObjectPath.eval("a.b.0.0.c", map), is((Object) "val"));
        assertThat(ObjectPath.eval("a.b.0.0.c.d", map), nullValue());
        assertThat(ObjectPath.eval("a.b.0.0.d", map), nullValue());
        assertThat(ObjectPath.eval("a.b.c", map), nullValue());

    }
}
