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

import org.elasticsearch.script.ExecutableScript;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BindingsTests extends ScriptTestCase {

    public void testBasicBinding() {
        assertEquals(15, exec("testAddWithState(4, 5, 6, 0.0)"));
    }

    public void testRepeatedBinding() {
        String script = "testAddWithState(4, 5, params.test, 0.0)";
        Map<String, Object> params = new HashMap<>();
        ExecutableScript.Factory factory = scriptEngine.compile(null, script, ExecutableScript.CONTEXT, Collections.emptyMap());
        ExecutableScript executableScript = factory.newInstance(params);

        executableScript.setNextVar("test", 5);
        assertEquals(14, executableScript.run());

        executableScript.setNextVar("test", 4);
        assertEquals(13, executableScript.run());

        executableScript.setNextVar("test", 7);
        assertEquals(16, executableScript.run());
    }

    public void testBoundBinding() {
        String script = "testAddWithState(4, params.bound, params.test, 0.0)";
        Map<String, Object> params = new HashMap<>();
        ExecutableScript.Factory factory = scriptEngine.compile(null, script, ExecutableScript.CONTEXT, Collections.emptyMap());
        ExecutableScript executableScript = factory.newInstance(params);

        executableScript.setNextVar("test", 5);
        executableScript.setNextVar("bound", 1);
        assertEquals(10, executableScript.run());

        executableScript.setNextVar("test", 4);
        executableScript.setNextVar("bound", 2);
        assertEquals(9, executableScript.run());
    }
}
