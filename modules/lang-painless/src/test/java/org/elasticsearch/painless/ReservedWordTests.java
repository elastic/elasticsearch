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

package org.elasticsearch.painless;

import java.util.Collections;
import java.util.HashMap;

/** Tests for special reserved words such as _score */
public class ReservedWordTests extends ScriptTestCase {

    /** check that we can't declare a variable of _score, its really reserved! */
    public void testScoreVar() {
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("int _score = 5; return _score;");
        });
        assertTrue(expected.getMessage().contains("Variable [_score] is reserved"));
    }

    /** check that we can't write to _score, its read-only! */
    public void testScoreStore() {
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("_score = 5; return _score;");
        });
        assertTrue(expected.getMessage().contains("Variable [_score] is read-only"));
    }

    /** check that we can't declare a variable of doc, its really reserved! */
    public void testDocVar() {
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("int doc = 5; return doc;");
        });
        assertTrue(expected.getMessage().contains("Variable [doc] is reserved"));
    }

    /** check that we can't write to doc, its read-only! */
    public void testDocStore() {
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("doc = 5; return doc;");
        });
        assertTrue(expected.getMessage().contains("Variable [doc] is read-only"));
    }

    /** check that we can't declare a variable of ctx, its really reserved! */
    public void testCtxVar() {
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("int ctx = 5; return ctx;");
        });
        assertTrue(expected.getMessage().contains("Variable [ctx] is reserved"));
    }

    /** check that we can't write to ctx, its read-only! */
    public void testCtxStore() {
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("ctx = 5; return ctx;");
        });
        assertTrue(expected.getMessage().contains("Variable [ctx] is read-only"));
    }

    /** check that we can modify its contents though */
    public void testCtxStoreMap() {
        assertEquals(5, exec("ctx.foo = 5; return ctx.foo;", Collections.singletonMap("ctx", new HashMap<String,Object>())));
    }

    /** check that we can't declare a variable of _value, its really reserved! */
    public void testAggregationValueVar() {
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("int _value = 5; return _value;");
        });
        assertTrue(expected.getMessage().contains("Variable [_value] is reserved"));
    }

    /** check that we can't write to _value, its read-only! */
    public void testAggregationValueStore() {
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("_value = 5; return _value;");
        });
        assertTrue(expected.getMessage().contains("Variable [_value] is read-only"));
    }
}
