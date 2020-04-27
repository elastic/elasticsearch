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

package org.elasticsearch.script;

import org.elasticsearch.common.io.stream.DataOutputStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Simple tests for {@link ScriptException} */
public class ScriptExceptionTests extends ESTestCase {

    /** ensure we can round trip in serialization */
    public void testRoundTrip() throws IOException {
        ScriptException e = new ScriptException("messageData", new Exception("causeData"), Arrays.asList("stack1", "stack2"),
                                                "sourceData", "langData");

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        StreamOutput output = new DataOutputStreamOutput(new DataOutputStream(bytes));
        e.writeTo(output);
        output.close();

        StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(bytes.toByteArray()));
        ScriptException e2 = new ScriptException(input);
        input.close();

        assertEquals(e.getMessage(), e2.getMessage());
        assertEquals(e.getScriptStack(), e2.getScriptStack());
        assertEquals(e.getScript(), e2.getScript());
        assertEquals(e.getLang(), e2.getLang());
        assertNull(e.getPos());

        // Ensure non-null position also works
        e = new ScriptException(e.getMessage(), e.getCause(), e.getScriptStack(), e.getScript(), e.getLang(),
            new ScriptException.Position(1, 0, 2));
        bytes = new ByteArrayOutputStream();
        output = new DataOutputStreamOutput(new DataOutputStream(bytes));
        e.writeTo(output);
        output.close();

        input = new InputStreamStreamInput(new ByteArrayInputStream(bytes.toByteArray()));
        e2 = new ScriptException(input);
        input.close();
        assertEquals(e.getPos(), e2.getPos());
    }

    /** Test that our elements are present in the json output */
    public void testJsonOutput() {
        ScriptException e = new ScriptException("messageData", new Exception("causeData"), Arrays.asList("stack1", "stack2"),
                                                "sourceData", "langData", new ScriptException.Position(2, 1, 3));
        String json = e.toJsonString();
        assertTrue(json.contains(e.getMessage()));
        assertTrue(json.contains(e.getCause().getMessage()));
        assertTrue(json.contains("stack1"));
        assertTrue(json.contains("stack2"));
        assertTrue(json.contains(e.getScript()));
        assertTrue(json.contains(e.getLang()));
        assertTrue(json.contains("1"));
        assertTrue(json.contains("2"));
        assertTrue(json.contains("3"));
    }

    /** ensure the script stack is immutable */
    public void testImmutableStack() {
        ScriptException e = new ScriptException("a", new Exception(), Arrays.asList("element1", "element2"), "a", "b");
        List<String> stack = e.getScriptStack();
        expectThrows(UnsupportedOperationException.class, () -> {
            stack.add("no");
        });
    }

    /** ensure no parameters can be null except pos*/
    public void testNoLeniency() {
        expectThrows(NullPointerException.class, () -> {
           new ScriptException(null, new Exception(), Collections.emptyList(), "a", "b");
        });
        expectThrows(NullPointerException.class, () -> {
            new ScriptException("test", null, Collections.emptyList(), "a", "b");
        });
        expectThrows(NullPointerException.class, () -> {
            new ScriptException("test", new Exception(), null, "a", "b");
        });
        expectThrows(NullPointerException.class, () -> {
            new ScriptException("test", new Exception(), Collections.emptyList(), null, "b");
        });
        expectThrows(NullPointerException.class, () -> {
            new ScriptException("test", new Exception(), Collections.emptyList(), "a", null);
        });
    }
}
