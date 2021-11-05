/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class JsonTests extends ScriptTestCase {

    public void testDump() {
        // simple object dump
        Object output = exec("Json.dump(params.data)", singletonMap("data", singletonMap("hello", "world")), true);
        assertEquals("{\"hello\":\"world\"}", output);

        output = exec("Json.dump(params.data)", singletonMap("data", singletonList(42)), true);
        assertEquals("[42]", output);

        // pretty print
        output = exec("Json.dump(params.data, true)", singletonMap("data", singletonMap("hello", "world")), true);
        assertEquals("{\n  \"hello\" : \"world\"\n}", output);
    }

    public void testLoad() {
        String json = "{\"hello\":\"world\"}";
        Object output = exec("Json.load(params.json)", singletonMap("json", json), true);
        assertEquals(singletonMap("hello", "world"), output);

        json = "[42]";
        output = exec("Json.load(params.json)", singletonMap("json", json), true);
        assertEquals(singletonList(42), output);
    }

}
