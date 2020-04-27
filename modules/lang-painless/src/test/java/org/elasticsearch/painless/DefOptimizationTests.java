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

        final Exception exception = expectScriptThrows(ClassCastException.class, () -> {
            exec(script);
        });
        assertTrue(exception.getMessage().contains("Cannot cast java.lang.Double to java.lang.Integer"));
    }
    
    public void testMulOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x * y", 
                             "INVOKEDYNAMIC mul(ILjava/lang/Object;)Ljava/lang/Object;");
    }
    
    public void testMulOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x * y", 
                             "INVOKEDYNAMIC mul(Ljava/lang/Object;I)Ljava/lang/Object;");
    }
    
    public void testMulOptRet() {
        assertBytecodeExists("def x = 1; def y = 2; double d = x * y", 
                             "INVOKEDYNAMIC mul(Ljava/lang/Object;Ljava/lang/Object;)D");
    }
    
    public void testDivOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x / y", 
                             "INVOKEDYNAMIC div(ILjava/lang/Object;)Ljava/lang/Object;");
    }
    
    public void testDivOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x / y", 
                             "INVOKEDYNAMIC div(Ljava/lang/Object;I)Ljava/lang/Object;");
    }
    
    public void testDivOptRet() {
        assertBytecodeExists("def x = 1; def y = 2; double d = x / y", 
                             "INVOKEDYNAMIC div(Ljava/lang/Object;Ljava/lang/Object;)D");
    }
    
    public void testRemOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x % y", 
                             "INVOKEDYNAMIC rem(ILjava/lang/Object;)Ljava/lang/Object;");
    }
    
    public void testRemOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x % y", 
                             "INVOKEDYNAMIC rem(Ljava/lang/Object;I)Ljava/lang/Object;");
    }
    
    public void testRemOptRet() {
        assertBytecodeExists("def x = 1; def y = 2; double d = x % y", 
                             "INVOKEDYNAMIC rem(Ljava/lang/Object;Ljava/lang/Object;)D");
    }
    
    public void testAddOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x + y", 
                             "INVOKEDYNAMIC add(ILjava/lang/Object;)Ljava/lang/Object;");
    }
    
    public void testAddOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x + y", 
                             "INVOKEDYNAMIC add(Ljava/lang/Object;I)Ljava/lang/Object;");
    }
    
    public void testAddOptRet() {
        assertBytecodeExists("def x = 1; def y = 2; double d = x + y", 
                             "INVOKEDYNAMIC add(Ljava/lang/Object;Ljava/lang/Object;)D");
    }
    
    // horrible, sorry
    public void testAddOptNullGuards() {
        // needs null guard
        assertBytecodeHasPattern("def x = 1; def y = 2; return x + y", 
                "(?s).*INVOKEDYNAMIC add.*arguments:\\s+" + "\\d+"
                                                + ",\\s+" + DefBootstrap.BINARY_OPERATOR 
                                                + ",\\s+" + DefBootstrap.OPERATOR_ALLOWS_NULL + ".*");
        // still needs null guard, NPE is the wrong thing!
        assertBytecodeHasPattern("def x = 1; def y = 2; double z = x + y", 
                "(?s).*INVOKEDYNAMIC add.*arguments:\\s+" + "\\d+"
                                                + ",\\s+" + DefBootstrap.BINARY_OPERATOR
                                                + ",\\s+" + DefBootstrap.OPERATOR_ALLOWS_NULL + ".*");
        // a primitive argument is present: no null guard needed
        assertBytecodeHasPattern("def x = 1; int y = 2; return x + y", 
                "(?s).*INVOKEDYNAMIC add.*arguments:\\s+" + "\\d+"
                                                + ",\\s+" + DefBootstrap.BINARY_OPERATOR
                                                + ",\\s+" + 0 + ".*");
        assertBytecodeHasPattern("int x = 1; def y = 2; return x + y", 
                "(?s).*INVOKEDYNAMIC add.*arguments:\\s+" + "\\d+"
                                                + ",\\s+" + DefBootstrap.BINARY_OPERATOR
                                                + ",\\s+" + 0 + ".*");
    }
    
    public void testSubOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x - y", 
                             "INVOKEDYNAMIC sub(ILjava/lang/Object;)Ljava/lang/Object;");
    }
    
    public void testSubOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x - y", 
                             "INVOKEDYNAMIC sub(Ljava/lang/Object;I)Ljava/lang/Object;");
    }
    
    public void testSubOptRet() {
        assertBytecodeExists("def x = 1; def y = 2; double d = x - y", 
                             "INVOKEDYNAMIC sub(Ljava/lang/Object;Ljava/lang/Object;)D");
    }
    
    public void testLshOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x << y", 
                             "INVOKEDYNAMIC lsh(ILjava/lang/Object;)Ljava/lang/Object;");
    }
    
    public void testLshOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x << y", 
                             "INVOKEDYNAMIC lsh(Ljava/lang/Object;I)Ljava/lang/Object;");
    }
    
    public void testLshOptRet() {
        assertBytecodeExists("def x = 1; def y = 2; double d = x << y", 
                             "INVOKEDYNAMIC lsh(Ljava/lang/Object;Ljava/lang/Object;)D");
    }
    
    public void testRshOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x >> y", 
                             "INVOKEDYNAMIC rsh(ILjava/lang/Object;)Ljava/lang/Object;");
    }
    
    public void testRshOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x >> y", 
                             "INVOKEDYNAMIC rsh(Ljava/lang/Object;I)Ljava/lang/Object;");
    }
    
    public void testRshOptRet() {
        assertBytecodeExists("def x = 1; def y = 2; double d = x >> y", 
                             "INVOKEDYNAMIC rsh(Ljava/lang/Object;Ljava/lang/Object;)D");
    }
    
    public void testUshOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x >>> y", 
                             "INVOKEDYNAMIC ush(ILjava/lang/Object;)Ljava/lang/Object;");
    }
    
    public void testUshOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x >>> y", 
                             "INVOKEDYNAMIC ush(Ljava/lang/Object;I)Ljava/lang/Object;");
    }
    
    public void testUshOptRet() {
        assertBytecodeExists("def x = 1; def y = 2; double d = x >>> y", 
                             "INVOKEDYNAMIC ush(Ljava/lang/Object;Ljava/lang/Object;)D");
    }
    
    public void testAndOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x & y", 
                             "INVOKEDYNAMIC and(ILjava/lang/Object;)Ljava/lang/Object;");
    }
    
    public void testAndOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x & y", 
                             "INVOKEDYNAMIC and(Ljava/lang/Object;I)Ljava/lang/Object;");
    }
    
    public void testAndOptRet() {
        assertBytecodeExists("def x = 1; def y = 2; double d = x & y", 
                             "INVOKEDYNAMIC and(Ljava/lang/Object;Ljava/lang/Object;)D");
    }
    
    public void testOrOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x | y", 
                             "INVOKEDYNAMIC or(ILjava/lang/Object;)Ljava/lang/Object;");
    }
    
    public void testOrOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x | y", 
                             "INVOKEDYNAMIC or(Ljava/lang/Object;I)Ljava/lang/Object;");
    }
    
    public void testOrOptRet() {
        assertBytecodeExists("def x = 1; def y = 2; double d = x | y", 
                             "INVOKEDYNAMIC or(Ljava/lang/Object;Ljava/lang/Object;)D");
    }
    
    public void testXorOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x ^ y", 
                             "INVOKEDYNAMIC xor(ILjava/lang/Object;)Ljava/lang/Object;");
    }
    
    public void testXorOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x ^ y", 
                             "INVOKEDYNAMIC xor(Ljava/lang/Object;I)Ljava/lang/Object;");
    }
    
    public void testXorOptRet() {
        assertBytecodeExists("def x = 1; def y = 2; double d = x ^ y", 
                             "INVOKEDYNAMIC xor(Ljava/lang/Object;Ljava/lang/Object;)D");
    }

    public void testBooleanXorOptLHS() {
        assertBytecodeExists("boolean x = true; def y = true; return x ^ y", 
                "INVOKEDYNAMIC xor(ZLjava/lang/Object;)Ljava/lang/Object;");
    }

    public void testBooleanXorOptRHS() {
        assertBytecodeExists("def x = true; boolean y = true; return x ^ y", 
                "INVOKEDYNAMIC xor(Ljava/lang/Object;Z)Ljava/lang/Object;");
    }
    
    public void testBooleanXorOptRet() {
        assertBytecodeExists("def x = true; def y = true; boolean v = x ^ y", 
                "INVOKEDYNAMIC xor(Ljava/lang/Object;Ljava/lang/Object;)Z");
    }

    public void testLtOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x < y", 
                             "INVOKEDYNAMIC lt(ILjava/lang/Object;)Z");
    }
    
    public void testLtOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x < y", 
                             "INVOKEDYNAMIC lt(Ljava/lang/Object;I)Z");
    }
    
    public void testLteOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x <= y", 
                             "INVOKEDYNAMIC lte(ILjava/lang/Object;)Z");
    }
    
    public void testLteOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x <= y", 
                             "INVOKEDYNAMIC lte(Ljava/lang/Object;I)Z");
    }
    
    public void testEqOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x == y", 
                             "INVOKEDYNAMIC eq(ILjava/lang/Object;)Z");
    }
    
    public void testEqOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x == y", 
                             "INVOKEDYNAMIC eq(Ljava/lang/Object;I)Z");
    }
    
    public void testNeqOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x != y", 
                             "INVOKEDYNAMIC eq(ILjava/lang/Object;)Z");
    }
    
    public void testNeqOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x != y", 
                             "INVOKEDYNAMIC eq(Ljava/lang/Object;I)Z");
    }
    
    public void testGteOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x >= y", 
                             "INVOKEDYNAMIC gte(ILjava/lang/Object;)Z");
    }
    
    public void testGteOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x >= y", 
                             "INVOKEDYNAMIC gte(Ljava/lang/Object;I)Z");
    }
    
    public void testGtOptLHS() {
        assertBytecodeExists("int x = 1; def y = 2; return x > y", 
                             "INVOKEDYNAMIC gt(ILjava/lang/Object;)Z");
    }
    
    public void testGtOptRHS() {
        assertBytecodeExists("def x = 1; int y = 2; return x > y", 
                             "INVOKEDYNAMIC gt(Ljava/lang/Object;I)Z");
    }
    
    public void testUnaryMinusOptRet() {
        assertBytecodeExists("def x = 1; double y = -x; return y", 
                             "INVOKEDYNAMIC neg(Ljava/lang/Object;)D");
    }
    
    public void testUnaryNotOptRet() {
        assertBytecodeExists("def x = 1; double y = ~x; return y", 
                             "INVOKEDYNAMIC not(Ljava/lang/Object;)D");
    }
    
    public void testUnaryPlusOptRet() {
        assertBytecodeExists("def x = 1; double y = +x; return y", 
                             "INVOKEDYNAMIC plus(Ljava/lang/Object;)D");
    }
    
    public void testLambdaReturnType() {
        assertBytecodeExists("List l = new ArrayList(); l.removeIf(x -> x < 10)",
                             "synthetic lambda$synthetic$0(Ljava/lang/Object;)Z");
    }
    
    public void testLambdaArguments() {
        assertBytecodeExists("List l = new ArrayList(); l.stream().mapToDouble(Double::valueOf).map(x -> x + 1)",
                             "synthetic lambda$synthetic$0(D)D");
    }
}
