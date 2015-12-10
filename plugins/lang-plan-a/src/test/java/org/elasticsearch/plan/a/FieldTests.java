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

package org.elasticsearch.plan.a;

import org.junit.Before;

public class FieldTests extends ScriptTestCase {
    public static class FieldClass {
        public boolean z = false;
        public byte b = 0;
        public short s = 1;
        public char c = 'c';
        public int i = 2;
        public int si = -1;
        public long j = 3l;
        public float f = 4.0f;
        public double d = 5.0;
        public String t = "s";
        public Object l = new Object();

        public float test(float a, float b) {
            return Math.min(a, b);
        }

        public int getSi() {
            return si;
        }

        public void setSi(final int si) {
            this.si = si;
        }
    }

    public static class FieldDefinition extends Definition {
        FieldDefinition() {
            super();

            addStruct("FieldClass", FieldClass.class);
            addConstructor("FieldClass", "new", new Type[] {}, null);
            addField("FieldClass", "z", null, false, booleanType, null);
            addField("FieldClass", "b", null, false, byteType, null);
            addField("FieldClass", "s", null, false, shortType, null);
            addField("FieldClass", "c", null, false, charType, null);
            addField("FieldClass", "i", null, false, intType, null);
            addField("FieldClass", "j", null, false, longType, null);
            addField("FieldClass", "f", null, false, floatType, null);
            addField("FieldClass", "d", null, false, doubleType, null);
            addField("FieldClass", "t", null, false, stringType, null);
            addField("FieldClass", "l", null, false, objectType, null);
            addClass("FieldClass");
            addMethod("FieldClass", "getSi", null, false, intType, new Type[] {}, null, null);
            addMethod("FieldClass", "setSi", null, false, voidType, new Type[] {intType}, null, null);
            addMethod("FieldClass", "test", null, false, floatType, new Type[] {floatType, floatType}, null, null);
        }
    }

    @Before
    public void setDefinition() {
        scriptEngine.setDefinition(new FieldDefinition());
    }

    public void testIntField() {
        assertEquals("s5t42", exec("def fc = new FieldClass() return fc.t += 2 + fc.j + \"t\" + 4 + (3 - 1)"));
        assertEquals(2.0f, exec("def fc = new FieldClass(); def l = new Double(3) Byte b = new Byte((byte)2) return fc.test(l, b)"));
        assertEquals(4, exec("def fc = new FieldClass() fc.i = 4 return fc.i"));
        assertEquals(5, exec("FieldClass fc0 = new FieldClass() FieldClass fc1 = new FieldClass() fc0.i = 7 - fc0.i fc1.i = fc0.i return fc1.i"));
        assertEquals(8, exec("def fc0 = new FieldClass() def fc1 = new FieldClass() fc0.i += fc1.i fc0.i += fc0.i return fc0.i"));
    }

    public void testExplicitShortcut() {
        assertEquals(5, exec("FieldClass fc = new FieldClass() fc.setSi(5) return fc.si"));
        assertEquals(-1, exec("FieldClass fc = new FieldClass() def x = fc.getSi() x"));
        assertEquals(5, exec("FieldClass fc = new FieldClass() fc.si = 5 return fc.si"));
        assertEquals(0, exec("FieldClass fc = new FieldClass() fc.si++ return fc.si"));
        assertEquals(-1, exec("FieldClass fc = new FieldClass() def x = fc.si++ return x"));
        assertEquals(0, exec("FieldClass fc = new FieldClass() def x = ++fc.si return x"));
        assertEquals(-2, exec("FieldClass fc = new FieldClass() fc.si *= 2 fc.si"));
        assertEquals("-1test", exec("FieldClass fc = new FieldClass() fc.si + \"test\""));
    }

    public void testImplicitShortcut() {
        assertEquals(5, exec("def fc = new FieldClass() fc.setSi(5) return fc.si"));
        assertEquals(-1, exec("def fc = new FieldClass() def x = fc.getSi() x"));
        assertEquals(5, exec("def fc = new FieldClass() fc.si = 5 return fc.si"));
        assertEquals(0, exec("def fc = new FieldClass() fc.si++ return fc.si"));
        assertEquals(-1, exec("def fc = new FieldClass() def x = fc.si++ return x"));
        assertEquals(0, exec("def fc = new FieldClass() def x = ++fc.si return x"));
        assertEquals(-2, exec("def fc = new FieldClass() fc.si *= 2 fc.si"));
        assertEquals("-1test", exec("def fc = new FieldClass() fc.si + \"test\""));
    }
}
