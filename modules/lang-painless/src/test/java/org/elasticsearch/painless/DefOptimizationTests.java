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

public class DefOptimizationTests extends ScriptTestCase {
    public void testIntBraceArrayOptiLoad() {
        final String script = "int x = 0; def y = new int[1]; y[0] = 5; x = y[0]; return x;";
        assertBytecodeExists(script, "INVOKEDYNAMIC arrayLoad(Ljava/lang/Object;I)I");
        assertEquals(5, exec(script));
    }

    public void testIntBraceArrayOptiStore() {
        final String script = "int x = 1; def y = new int[1]; y[0] = x; return y[0];";
        assertBytecodeExists(script, "INVOKEDYNAMIC arrayStore(Ljava/lang/Object;II)");
        assertEquals(1, exec(script));
    }

    public void testIntBraceListOptiLoad() {
        final String script = "int x = 0; def y = new ArrayList(); y.add(5); x = y[0]; return x;";
        assertBytecodeExists(script, "INVOKEDYNAMIC arrayLoad(Ljava/lang/Object;I)I");
        assertEquals(5, exec(script));
    }

    public void testIntBraceListOptiStore() {
        final String script = "int x = 1; def y = new ArrayList(); y.add(0); y[0] = x; return y[0];";
        assertBytecodeExists(script, "INVOKEDYNAMIC arrayStore(Ljava/lang/Object;II)");
        assertEquals(1, exec(script));
    }

    public void testIntBraceMapOptiLoad() {
        final String script = "int x = 0; def y = new HashMap(); y.put(0, 5); x = y[0];";
        assertBytecodeExists(script, "INVOKEDYNAMIC arrayLoad(Ljava/lang/Object;I)I");
        assertEquals(5, exec(script));
    }

    public void testIntBraceMapOptiStore() {
        final String script = "int x = 1; def y = new HashMap(); y.put(0, 1); y[0] = x;";
        assertBytecodeExists(script, "INVOKEDYNAMIC arrayStore(Ljava/lang/Object;II)");
        assertEquals(1, exec(script));
    }

    public void testIntFieldListOptiLoad() {
        final String script = "int x = 0; def y = new ArrayList(); y.add(5); x = y.0;";
        assertBytecodeExists(script, "INVOKEDYNAMIC 0(Ljava/lang/Object;)I");
        assertEquals(5, exec(script));
    }

    public void testIntFieldListOptiStore() {
        final String script = "int x = 1; def y = new ArrayList(); y.add(0); y.0 = x;";
        assertBytecodeExists(script, "INVOKEDYNAMIC 0(Ljava/lang/Object;I)");
        assertEquals(1, exec(script));
    }

    public void testIntFieldMapOptiLoad() {
        final String script = "int x = 0; def y = new HashMap(); y.put('0', 5); x = y.0; return x;";
        assertBytecodeExists(script, "INVOKEDYNAMIC 0(Ljava/lang/Object;)I");
        assertEquals(5, exec(script));
    }

    public void testIntFieldMapOptiStore() {
        final String script = "int x = 1; def y = new HashMap(); y.put('0', 1); y.0 = x; return y.0;";
        assertBytecodeExists(script, "INVOKEDYNAMIC 0(Ljava/lang/Object;I)");
        assertEquals(1, exec(script));
    }

    public void testIntCall0Opti() {
        final String script = "int x; def y = new HashMap(); y['int'] = 1; x = y.get('int'); return x;";
        assertBytecodeExists(script, "INVOKEDYNAMIC get(Ljava/lang/Object;Ljava/lang/String;)I");
        assertEquals(1, exec(script));
    }

    public void testIntCall1Opti() {
        final String script = "int x; def y = new HashMap(); y['int'] = 1; x = y.get('int');";
        assertBytecodeExists(script, "INVOKEDYNAMIC get(Ljava/lang/Object;Ljava/lang/String;)I");
        assertEquals(1, exec(script));
    }

    public void testDoubleBraceArrayOptiLoad() {
        final String script = "double x = 0; def y = new double[1]; y[0] = 5.0; x = y[0]; return x;";
        assertBytecodeExists(script, "INVOKEDYNAMIC arrayLoad(Ljava/lang/Object;I)D");
        assertEquals(5.0, exec(script));
    }

    public void testDoubleBraceArrayOptiStore() {
        final String script = "double x = 1; def y = new double[1]; y[0] = x; return y[0];";

        assertBytecodeExists(script, "INVOKEDYNAMIC arrayStore(Ljava/lang/Object;ID)");
        assertEquals(1.0, exec(script));
    }

    public void testDoubleBraceListOptiLoad() {
        final String script = "double x = 0.0; def y = new ArrayList(); y.add(5.0); x = y[0]; return x;";

        assertBytecodeExists(script, "INVOKEDYNAMIC arrayLoad(Ljava/lang/Object;I)D");
        assertEquals(5.0, exec(script));
    }

    public void testDoubleBraceListOptiStore() {
        final String script = "double x = 1.0; def y = new ArrayList(); y.add(0.0); y[0] = x; return y[0];";

        assertBytecodeExists(script, "INVOKEDYNAMIC arrayStore(Ljava/lang/Object;ID)");
        assertEquals(1.0, exec(script));
    }

    public void testDoubleBraceMapOptiLoad() {
        final String script = "double x = 0.0; def y = new HashMap(); y.put(0, 5.0); x = y[0];";

        assertBytecodeExists(script, "INVOKEDYNAMIC arrayLoad(Ljava/lang/Object;I)D");
        assertEquals(5.0, exec(script));
    }

    public void testDoubleBraceMapOptiStore() {
        final String script = "double x = 1.0; def y = new HashMap(); y.put(0, 2.0); y[0] = x;";

        assertBytecodeExists(script, "INVOKEDYNAMIC arrayStore(Ljava/lang/Object;ID)");
        assertEquals(1.0, exec(script));
    }

    public void testDoubleFieldListOptiLoad() {
        final String script = "double x = 0; def y = new ArrayList(); y.add(5.0); x = y.0;";
        assertBytecodeExists(script, "INVOKEDYNAMIC 0(Ljava/lang/Object;)D");
        assertEquals(5.0, exec(script));
    }

    public void testDoubleFieldListOptiStore() {
        final String script = "double x = 1.0; def y = new ArrayList(); y.add(0); y.0 = x;";
        assertBytecodeExists(script, "INVOKEDYNAMIC 0(Ljava/lang/Object;D)");
        assertEquals(1.0, exec(script));
    }

    public void testDoubleFieldMapOptiLoad() {
        final String script = "double x = 0; def y = new HashMap(); y.put('0', 5.0); x = y.0; return x;";
        assertBytecodeExists(script, "INVOKEDYNAMIC 0(Ljava/lang/Object;)D");
        assertEquals(5.0, exec(script));
    }

    public void testDoubleFieldMapOptiStore() {
        final String script = "double x = 1.0; def y = new HashMap(); y.put('0', 1.0); y.0 = x; return y.0;";
        assertBytecodeExists(script, "INVOKEDYNAMIC 0(Ljava/lang/Object;D)");
        assertEquals(1.0, exec(script));
    }

    public void testDoubleCall0Opti() {
        final String script = "double x; def y = new HashMap(); y['double'] = 1.0; x = y.get('double'); return x;";
        assertBytecodeExists(script, "INVOKEDYNAMIC get(Ljava/lang/Object;Ljava/lang/String;)D");
        assertEquals(1.0, exec(script));
    }

    public void testDoubleCall1Opti() {
        final String script = "double x; def y = new HashMap(); y['double'] = 1.0; x = y.get('double');";
        assertBytecodeExists(script, "INVOKEDYNAMIC get(Ljava/lang/Object;Ljava/lang/String;)D");
        assertEquals(1.0, exec(script));
    }

    public void testIllegalCast() {
        final String script = "int x;\ndef y = new HashMap();\ny['double'] = 1.0;\nx = y.get('double');\n";
        assertBytecodeExists(script, "INVOKEDYNAMIC get(Ljava/lang/Object;Ljava/lang/String;)I");

        final Exception exception = expectThrows(ClassCastException.class, () -> {
            exec(script);
        });
        assertTrue(exception.getMessage().contains("Cannot cast java.lang.Double to java.lang.Integer"));
    }
}
