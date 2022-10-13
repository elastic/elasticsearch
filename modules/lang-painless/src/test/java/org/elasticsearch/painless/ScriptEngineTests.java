/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ScriptEngineTests extends ScriptTestCase {

    public void testSimpleEquation() {
        final Object value = exec("return 1 + 2;");
        assertEquals(3, ((Number) value).intValue());
    }

    @SuppressWarnings("unchecked") // We know its Map<String, Object> because we put them there in the test
    public void testMapAccess() {
        Map<String, Object> vars = new HashMap<>();
        Map<String, Object> obj2 = new HashMap<>();
        obj2.put("prop2", "value2");
        Map<String, Object> obj1 = new HashMap<>();
        obj1.put("prop1", "value1");
        obj1.put("obj2", obj2);
        obj1.put("l", Arrays.asList("2", "1"));
        vars.put("obj1", obj1);

        Object value = exec("return params['obj1'];", vars, true);
        obj1 = (Map<String, Object>) value;
        assertEquals("value1", obj1.get("prop1"));
        assertEquals("value2", ((Map<String, Object>) obj1.get("obj2")).get("prop2"));

        value = exec("return params.obj1.l.0;", vars, true);
        assertEquals("2", value);
    }

    @SuppressWarnings("unchecked") // We know its Map<String, Object> because we put them there ourselves
    public void testAccessListInScript() {
        Map<String, Object> vars = new HashMap<>();
        Map<String, Object> obj2 = new HashMap<>();
        obj2.put("prop2", "value2");
        Map<String, Object> obj1 = new HashMap<>();
        obj1.put("prop1", "value1");
        obj1.put("obj2", obj2);
        vars.put("l", Arrays.asList("1", "2", "3", obj1));

        assertEquals(4, exec("return params.l.size();", vars, true));
        assertEquals("1", exec("return params.l.0;", vars, true));

        Object value = exec("return params.l.3;", vars, true);
        obj1 = (Map<String, Object>) value;
        assertEquals("value1", obj1.get("prop1"));
        assertEquals("value2", ((Map<String, Object>) obj1.get("obj2")).get("prop2"));

        assertEquals("value1", exec("return params.l.3.prop1;", vars, true));
    }
}
