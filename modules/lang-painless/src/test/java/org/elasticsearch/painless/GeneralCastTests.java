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

/** Tests for explicit casts */
public class GeneralCastTests extends ScriptTestCase {

    /**
     * Unary operator with explicit cast
     */
    public void testUnaryOperator() {
        assertEquals((byte)5, exec("long x = 5L; return (byte) (+x);"));
        assertEquals((short)5, exec("long x = 5L; return (short) (+x);"));
        assertEquals((char)5, exec("long x = 5L; return (char) (+x);"));
        assertEquals(5, exec("long x = 5L; return (int) (+x);"));
        assertEquals(5F, exec("long x = 5L; return (float) (+x);"));
        assertEquals(5L, exec("long x = 5L; return (long) (+x);"));
        assertEquals(5D, exec("long x = 5L; return (double) (+x);"));
    }

    /**
     * Binary operators with explicit cast
     */
    public void testBinaryOperator() {
        assertEquals((byte)6, exec("long x = 5L; return (byte) (x + 1);"));
        assertEquals((short)6, exec("long x = 5L; return (short) (x + 1);"));
        assertEquals((char)6, exec("long x = 5L; return (char) (x + 1);"));
        assertEquals(6, exec("long x = 5L; return (int) (x + 1);"));
        assertEquals(6F, exec("long x = 5L; return (float) (x + 1);"));
        assertEquals(6L, exec("long x = 5L; return (long) (x + 1);"));
        assertEquals(6D, exec("long x = 5L; return (double) (x + 1);"));
    }

    /**
     * Binary compound assignment with explicit cast
     */
    public void testBinaryCompoundAssignment() {
        assertEquals((byte)6, exec("long x = 5L; return (byte) (x += 1);"));
        assertEquals((short)6, exec("long x = 5L; return (short) (x += 1);"));
        assertEquals((char)6, exec("long x = 5L; return (char) (x += 1);"));
        assertEquals(6, exec("long x = 5L; return (int) (x += 1);"));
        assertEquals(6F, exec("long x = 5L; return (float) (x += 1);"));
        assertEquals(6L, exec("long x = 5L; return (long) (x += 1);"));
        assertEquals(6D, exec("long x = 5L; return (double) (x += 1);"));
    }

    /**
     * Binary compound prefix with explicit cast
     */
    public void testBinaryPrefix() {
        assertEquals((byte)6, exec("long x = 5L; return (byte) (++x);"));
        assertEquals((short)6, exec("long x = 5L; return (short) (++x);"));
        assertEquals((char)6, exec("long x = 5L; return (char) (++x);"));
        assertEquals(6, exec("long x = 5L; return (int) (++x);"));
        assertEquals(6F, exec("long x = 5L; return (float) (++x);"));
        assertEquals(6L, exec("long x = 5L; return (long) (++x);"));
        assertEquals(6D, exec("long x = 5L; return (double) (++x);"));
    }

    /**
     * Binary compound postifx with explicit cast
     */
    public void testBinaryPostfix() {
        assertEquals((byte)5, exec("long x = 5L; return (byte) (x++);"));
        assertEquals((short)5, exec("long x = 5L; return (short) (x++);"));
        assertEquals((char)5, exec("long x = 5L; return (char) (x++);"));
        assertEquals(5, exec("long x = 5L; return (int) (x++);"));
        assertEquals(5F, exec("long x = 5L; return (float) (x++);"));
        assertEquals(5L, exec("long x = 5L; return (long) (x++);"));
        assertEquals(5D, exec("long x = 5L; return (double) (x++);"));
    }

    /**
     * Shift operators with explicit cast
     */
    public void testShiftOperator() {
        assertEquals((byte)10, exec("long x = 5L; return (byte) (x << 1);"));
        assertEquals((short)10, exec("long x = 5L; return (short) (x << 1);"));
        assertEquals((char)10, exec("long x = 5L; return (char) (x << 1);"));
        assertEquals(10, exec("long x = 5L; return (int) (x << 1);"));
        assertEquals(10F, exec("long x = 5L; return (float) (x << 1);"));
        assertEquals(10L, exec("long x = 5L; return (long) (x << 1);"));
        assertEquals(10D, exec("long x = 5L; return (double) (x << 1);"));
    }

    /**
     * Shift compound assignment with explicit cast
     */
    public void testShiftCompoundAssignment() {
        assertEquals((byte)10, exec("long x = 5L; return (byte) (x <<= 1);"));
        assertEquals((short)10, exec("long x = 5L; return (short) (x <<= 1);"));
        assertEquals((char)10, exec("long x = 5L; return (char) (x <<= 1);"));
        assertEquals(10, exec("long x = 5L; return (int) (x <<= 1);"));
        assertEquals(10F, exec("long x = 5L; return (float) (x <<= 1);"));
        assertEquals(10L, exec("long x = 5L; return (long) (x <<= 1);"));
        assertEquals(10D, exec("long x = 5L; return (double) (x <<= 1);"));
    }

    /**
     * Test that without a cast, we fail when conversions would narrow.
     */
    public void testIllegalConversions() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("long x = 5L; int y = +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("long x = 5L; int y = (x + x); return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("boolean x = true; int y = +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("boolean x = true; int y = (x ^ false); return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("long x = 5L; boolean y = +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("long x = 5L; boolean y = (x + x); return y");
        });
    }

    /**
     * Test that even with a cast, some things aren't allowed.
     */
    public void testIllegalExplicitConversions() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("boolean x = true; int y = (int) +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("boolean x = true; int y = (int) (x ^ false); return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("long x = 5L; boolean y = (boolean) +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("long x = 5L; boolean y = (boolean) (x + x); return y");
        });
    }

    /**
     * Currently these do not adopt the return value, we issue a separate cast!
     */
    public void testMethodCallDef() {
        assertEquals(5, exec("def x = 5; return (int)x.longValue();"));
    }

    /**
     * Currently these do not adopt the argument value, we issue a separate cast!
     */
    public void testArgumentsDef() {
        assertEquals(5, exec("def x = 5L; return (+(int)x);"));
        assertEquals(6, exec("def x = 5; def y = 1L; return x + (int)y"));
        assertEquals('b', exec("def x = 'abcdeg'; def y = 1L; x.charAt((int)y)"));
    }

    /**
     * Unary operators adopt the return value
     */
    public void testUnaryOperatorDef() {
        assertEquals((byte)5, exec("def x = 5L; return (byte) (+x);"));
        assertEquals((short)5, exec("def x = 5L; return (short) (+x);"));
        assertEquals((char)5, exec("def x = 5L; return (char) (+x);"));
        assertEquals(5, exec("def x = 5L; return (int) (+x);"));
        assertEquals(5F, exec("def x = 5L; return (float) (+x);"));
        assertEquals(5L, exec("def x = 5L; return (long) (+x);"));
        assertEquals(5D, exec("def x = 5L; return (double) (+x);"));
    }

    /**
     * Binary operators adopt the return value
     */
    public void testBinaryOperatorDef() {
        assertEquals((byte)6, exec("def x = 5L; return (byte) (x + 1);"));
        assertEquals((short)6, exec("def x = 5L; return (short) (x + 1);"));
        assertEquals((char)6, exec("def x = 5L; return (char) (x + 1);"));
        assertEquals(6, exec("def x = 5L; return (int) (x + 1);"));
        assertEquals(6F, exec("def x = 5L; return (float) (x + 1);"));
        assertEquals(6L, exec("def x = 5L; return (long) (x + 1);"));
        assertEquals(6D, exec("def x = 5L; return (double) (x + 1);"));
    }

    /**
     * Binary operators don't yet adopt the return value with compound assignment
     */
    public void testBinaryCompoundAssignmentDef() {
        assertEquals((byte)6, exec("def x = 5L; return (byte) (x += 1);"));
        assertEquals((short)6, exec("def x = 5L; return (short) (x += 1);"));
        assertEquals((char)6, exec("def x = 5L; return (char) (x += 1);"));
        assertEquals(6, exec("def x = 5L; return (int) (x += 1);"));
        assertEquals(6F, exec("def x = 5L; return (float) (x += 1);"));
        assertEquals(6L, exec("def x = 5L; return (long) (x += 1);"));
        assertEquals(6D, exec("def x = 5L; return (double) (x += 1);"));
    }

    /**
     * Binary operators don't yet adopt the return value with compound assignment
     */
    public void testBinaryCompoundAssignmentPrefix() {
        assertEquals((byte)6, exec("def x = 5L; return (byte) (++x);"));
        assertEquals((short)6, exec("def x = 5L; return (short) (++x);"));
        assertEquals((char)6, exec("def x = 5L; return (char) (++x);"));
        assertEquals(6, exec("def x = 5L; return (int) (++x);"));
        assertEquals(6F, exec("def x = 5L; return (float) (++x);"));
        assertEquals(6L, exec("def x = 5L; return (long) (++x);"));
        assertEquals(6D, exec("def x = 5L; return (double) (++x);"));
    }

    /**
     * Binary operators don't yet adopt the return value with compound assignment
     */
    public void testBinaryCompoundAssignmentPostfix() {
        assertEquals((byte)5, exec("def x = 5L; return (byte) (x++);"));
        assertEquals((short)5, exec("def x = 5L; return (short) (x++);"));
        assertEquals((char)5, exec("def x = 5L; return (char) (x++);"));
        assertEquals(5, exec("def x = 5L; return (int) (x++);"));
        assertEquals(5F, exec("def x = 5L; return (float) (x++);"));
        assertEquals(5L, exec("def x = 5L; return (long) (x++);"));
        assertEquals(5D, exec("def x = 5L; return (double) (x++);"));
    }

    /**
     * Shift operators adopt the return value
     */
    public void testShiftOperatorDef() {
        assertEquals((byte)10, exec("def x = 5L; return (byte) (x << 1);"));
        assertEquals((short)10, exec("def x = 5L; return (short) (x << 1);"));
        assertEquals((char)10, exec("def x = 5L; return (char) (x << 1);"));
        assertEquals(10, exec("def x = 5L; return (int) (x << 1);"));
        assertEquals(10F, exec("def x = 5L; return (float) (x << 1);"));
        assertEquals(10L, exec("def x = 5L; return (long) (x << 1);"));
        assertEquals(10D, exec("def x = 5L; return (double) (x << 1);"));
    }

    /**
     * Shift operators don't yet adopt the return value with compound assignment
     */
    public void testShiftCompoundAssignmentDef() {
        assertEquals((byte)10, exec("def x = 5L; return (byte) (x <<= 1);"));
        assertEquals((short)10, exec("def x = 5L; return (short) (x <<= 1);"));
        assertEquals((char)10, exec("def x = 5L; return (char) (x <<= 1);"));
        assertEquals(10, exec("def x = 5L; return (int) (x <<= 1);"));
        assertEquals(10F, exec("def x = 5L; return (float) (x <<= 1);"));
        assertEquals(10L, exec("def x = 5L; return (long) (x <<= 1);"));
        assertEquals(10D, exec("def x = 5L; return (double) (x <<= 1);"));
    }

    /**
     * Test that without a cast, we fail when conversions would narrow.
     */
    public void testIllegalConversionsDef() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 5L; int y = +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 5L; int y = (x + x); return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = true; int y = +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = true; int y = (x ^ false); return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 5L; boolean y = +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 5L; boolean y = (x + x); return y");
        });
    }

    public void testUnboxMethodParameters() {
        assertEquals('a', exec("'a'.charAt(Integer.valueOf(0))"));
    }

    public void testIllegalCastInMethodArgument() {
        assertEquals('a', exec("'a'.charAt(0)"));
        Exception e = expectScriptThrows(ClassCastException.class, () -> exec("'a'.charAt(0L)"));
        assertEquals("Cannot cast from [long] to [int].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () -> exec("'a'.charAt(0.0f)"));
        assertEquals("Cannot cast from [float] to [int].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () -> exec("'a'.charAt(0.0d)"));
        assertEquals("Cannot cast from [double] to [int].", e.getMessage());
    }

    /**
     * Test that even with a cast, some things aren't allowed.
     * (stuff that methodhandles explicitCastArguments would otherwise allow)
     */
    public void testIllegalExplicitConversionsDef() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = true; int y = (int) +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = true; int y = (int) (x ^ false); return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 5L; boolean y = (boolean) +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 5L; boolean y = (boolean) (x + x); return y");
        });
    }

    public void testIllegalVoidCasts() {
        expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("def map = ['a': 1,'b': 2,'c': 3]; map.c = Collections.sort(new ArrayList(map.keySet()));");
        });
        expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("Map map = ['a': 1,'b': 2,'c': 3]; def x = new HashMap(); x.put(1, map.clear());");
        });
    }

    public void testBoxedDefCalls() {
        assertEquals(1, exec("def x = 1; def y = 2.0; y.compareTo(x);"));
        assertEquals(1, exec("def y = 2.0; y.compareTo(1);"));
        assertEquals(1, exec("int x = 1; def y = 2.0; y.compareTo(x);"));
        assertEquals(-1, exec("Integer x = Integer.valueOf(3); def y = 2.0; y.compareTo(x);"));
        assertEquals(2, exec("def f = new org.elasticsearch.painless.FeatureTestObject(); f.i = (byte)2; f.i"));
        assertEquals(4.0, exec(
                "def x = new org.elasticsearch.painless.FeatureTestObject(); " +
                "Byte i = Byte.valueOf(3); " +
                "byte j = 1;" +
                "Short s = Short.valueOf(-2);" +
                "x.mixedAdd(j, i, (char)2, s)"
        ));
        assertNull(exec("def f = new org.elasticsearch.painless.FeatureTestObject(); f.i = null; f.i"));
        expectScriptThrows(ClassCastException.class, () -> exec("def x = 2.0; def y = 1; y.compareTo(x);"));
        expectScriptThrows(ClassCastException.class, () -> exec("float f = 1.0f; def y = 1; y.compareTo(f);"));
    }
}
