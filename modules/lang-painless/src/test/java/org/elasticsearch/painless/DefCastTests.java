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

public class DefCastTests extends ScriptTestCase {

    public void testdefTobooleanImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; boolean b = d;"));
        assertEquals(true, exec("def d = true; boolean b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (byte)0; boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (short)0; boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (char)0; boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (int)0; boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (long)0; boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; boolean b = d;"));
        assertEquals(false, exec("def d = Boolean.valueOf(false); boolean b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Byte.valueOf(0); boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Short.valueOf(0); boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Character.valueOf(0); boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Integer.valueOf(0); boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Long.valueOf(0); boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); boolean b = d;"));
    }

    public void testdefTobyteImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; byte b = d;"));
        assertEquals((byte)0, exec("def d = (byte)0; byte b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (short)0; byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (char)0; byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (int)0; byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (long)0; byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); byte b = d;"));
        assertEquals((byte)0, exec("def d = Byte.valueOf(0); byte b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Short.valueOf(0); byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Character.valueOf(0); byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Integer.valueOf(0); byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Long.valueOf(0); byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); byte b = d;"));
    }

    public void testdefToshortImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; short b = d;"));
        assertEquals((short)0, exec("def d = (byte)0; short b = d; b"));
        assertEquals((short)0, exec("def d = (short)0; short b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (char)0; short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (int)0; short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (long)0; short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); short b = d;"));
        assertEquals((short)0, exec("def d = Byte.valueOf(0); short b = d; b"));
        assertEquals((short)0, exec("def d = Short.valueOf(0); short b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Character.valueOf(0); short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Integer.valueOf(0); short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Long.valueOf(0); short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); short b = d;"));
    }

    public void testdefTocharImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (byte)0; char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (short)0; char b = d;"));
        assertEquals((char)0, exec("def d = (char)0; char b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (int)0; char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (long)0; char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Byte.valueOf(0); char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Short.valueOf(0); char b = d;"));
        assertEquals((char)0, exec("def d = Character.valueOf(0); char b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Integer.valueOf(0); char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Long.valueOf(0); char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); char b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); char b = d;"));
    }

    public void testdefTointImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; int b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; int b = d;"));
        assertEquals(0, exec("def d = (byte)0; int b = d; b"));
        assertEquals(0, exec("def d = (short)0; int b = d; b"));
        assertEquals(0, exec("def d = (char)0; int b = d; b"));
        assertEquals(0, exec("def d = 0; int b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (long)0; int b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; int b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; int b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); int b = d;"));
        assertEquals(0, exec("def d = Byte.valueOf(0); int b = d; b"));
        assertEquals(0, exec("def d = Short.valueOf(0); int b = d; b"));
        assertEquals(0, exec("def d = Character.valueOf(0); int b = d; b"));
        assertEquals(0, exec("def d = Integer.valueOf(0); int b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Long.valueOf(0); int b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); int b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); int b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); int b = d;"));
    }

    public void testdefTolongImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; long b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; long b = d;"));
        assertEquals((long)0, exec("def d = (byte)0; long b = d; b"));
        assertEquals((long)0, exec("def d = (short)0; long b = d; b"));
        assertEquals((long)0, exec("def d = (char)0; long b = d; b"));
        assertEquals((long)0, exec("def d = 0; long b = d; b"));
        assertEquals((long)0, exec("def d = (long)0; long b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; long b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; long b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); long b = d;"));
        assertEquals((long)0, exec("def d = Byte.valueOf(0); long b = d; b"));
        assertEquals((long)0, exec("def d = Short.valueOf(0); long b = d; b"));
        assertEquals((long)0, exec("def d = Character.valueOf(0); long b = d; b"));
        assertEquals((long)0, exec("def d = Integer.valueOf(0); long b = d; b"));
        assertEquals((long)0, exec("def d = Long.valueOf(0); long b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); long b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); long b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); long b = d;"));
    }

    public void testdefTodoubleImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; double b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; double b = d;"));
        assertEquals((double)0, exec("def d = (byte)0; double b = d; b"));
        assertEquals((double)0, exec("def d = (short)0; double b = d; b"));
        assertEquals((double)0, exec("def d = (char)0; double b = d; b"));
        assertEquals((double)0, exec("def d = 0; double b = d; b"));
        assertEquals((double)0, exec("def d = (long)0; double b = d; b"));
        assertEquals((double)0, exec("def d = (float)0; double b = d; b"));
        assertEquals((double)0, exec("def d = (double)0; double b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); double b = d;"));
        assertEquals((double)0, exec("def d = Byte.valueOf(0); double b = d; b"));
        assertEquals((double)0, exec("def d = Short.valueOf(0); double b = d; b"));
        assertEquals((double)0, exec("def d = Character.valueOf(0); double b = d; b"));
        assertEquals((double)0, exec("def d = Integer.valueOf(0); double b = d; b"));
        assertEquals((double)0, exec("def d = Long.valueOf(0); double b = d; b"));
        assertEquals((double)0, exec("def d = Float.valueOf(0); double b = d; b"));
        assertEquals((double)0, exec("def d = Double.valueOf(0); double b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); double b = d;"));
    }

    public void testdefTobooleanExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; boolean b = (boolean)d;"));
        assertEquals(true, exec("def d = true; boolean b = (boolean)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (byte)0; boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (short)0; boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (char)0; boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (int)0; boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (long)0; boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; boolean b = (boolean)d;"));
        assertEquals(false, exec("def d = Boolean.valueOf(false); boolean b = (boolean)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Byte.valueOf(0); boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Short.valueOf(0); boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Character.valueOf(0); boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Integer.valueOf(0); boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Long.valueOf(0); boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); boolean b = (boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); boolean b = (boolean)d;"));
    }

    public void testdefTobyteExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; byte b = (byte)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; byte b = (byte)d;"));
        assertEquals((byte)0, exec("def d = (byte)0; byte b = (byte)d; b"));
        assertEquals((byte)0, exec("def d = (short)0; byte b = (byte)d; b"));
        assertEquals((byte)0, exec("def d = (char)0; byte b = (byte)d; b"));
        assertEquals((byte)0, exec("def d = 0; byte b = (byte)d; b"));
        assertEquals((byte)0, exec("def d = (long)0; byte b = (byte)d; b"));
        assertEquals((byte)0, exec("def d = (float)0; byte b = (byte)d; b"));
        assertEquals((byte)0, exec("def d = (double)0; byte b = (byte)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); byte b = d;"));
        assertEquals((byte)0, exec("def d = Byte.valueOf(0); byte b = (byte)d; b"));
        assertEquals((byte)0, exec("def d = Short.valueOf(0); byte b = (byte)d; b"));
        assertEquals((byte)0, exec("def d = Character.valueOf(0); byte b = (byte)d; b"));
        assertEquals((byte)0, exec("def d = Integer.valueOf(0); byte b = (byte)d; b"));
        assertEquals((byte)0, exec("def d = Long.valueOf(0); byte b = (byte)d; b"));
        assertEquals((byte)0, exec("def d = Float.valueOf(0); byte b = (byte)d; b"));
        assertEquals((byte)0, exec("def d = Double.valueOf(0); byte b = (byte)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); byte b = (byte)d;"));
    }

    public void testdefToshortExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; short b = (short)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; short b = (short)d;"));
        assertEquals((short)0, exec("def d = (byte)0; short b = (short)d; b"));
        assertEquals((short)0, exec("def d = (short)0; short b = (short)d; b"));
        assertEquals((short)0, exec("def d = (char)0; short b = (short)d; b"));
        assertEquals((short)0, exec("def d = 0; short b = (short)d; b"));
        assertEquals((short)0, exec("def d = (long)0; short b = (short)d; b"));
        assertEquals((short)0, exec("def d = (float)0; short b = (short)d; b"));
        assertEquals((short)0, exec("def d = (double)0; short b = (short)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); short b = d;"));
        assertEquals((short)0, exec("def d = Byte.valueOf(0); short b = (short)d; b"));
        assertEquals((short)0, exec("def d = Short.valueOf(0); short b = (short)d; b"));
        assertEquals((short)0, exec("def d = Character.valueOf(0); short b = (short)d; b"));
        assertEquals((short)0, exec("def d = Integer.valueOf(0); short b = (short)d; b"));
        assertEquals((short)0, exec("def d = Long.valueOf(0); short b = (short)d; b"));
        assertEquals((short)0, exec("def d = Float.valueOf(0); short b = (short)d; b"));
        assertEquals((short)0, exec("def d = Double.valueOf(0); short b = (short)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); short b = (short)d;"));
    }

    public void testdefTocharExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; char b = (char)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; char b = (char)d;"));
        assertEquals((char)0, exec("def d = (byte)0; char b = (char)d; b"));
        assertEquals((char)0, exec("def d = (short)0; char b = (char)d; b"));
        assertEquals((char)0, exec("def d = (char)0; char b = (char)d; b"));
        assertEquals((char)0, exec("def d = 0; char b = (char)d; b"));
        assertEquals((char)0, exec("def d = (long)0; char b = (char)d; b"));
        assertEquals((char)0, exec("def d = (float)0; char b = (char)d; b"));
        assertEquals((char)0, exec("def d = (double)0; char b = (char)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); char b = d;"));
        assertEquals((char)0, exec("def d = Byte.valueOf(0); char b = (char)d; b"));
        assertEquals((char)0, exec("def d = Short.valueOf(0); char b = (char)d; b"));
        assertEquals((char)0, exec("def d = Character.valueOf(0); char b = (char)d; b"));
        assertEquals((char)0, exec("def d = Integer.valueOf(0); char b = (char)d; b"));
        assertEquals((char)0, exec("def d = Long.valueOf(0); char b = (char)d; b"));
        assertEquals((char)0, exec("def d = Float.valueOf(0); char b = (char)d; b"));
        assertEquals((char)0, exec("def d = Double.valueOf(0); char b = (char)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); char b = (char)d;"));
    }

    public void testdefTointExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; int b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; int b = (int)d;"));
        assertEquals(0, exec("def d = (byte)0; int b = (int)d; b"));
        assertEquals(0, exec("def d = (short)0; int b = (int)d; b"));
        assertEquals(0, exec("def d = (char)0; int b = (int)d; b"));
        assertEquals(0, exec("def d = 0; int b = (int)d; b"));
        assertEquals(0, exec("def d = (long)0; int b = (int)d; b"));
        assertEquals(0, exec("def d = (float)0; int b = (int)d; b"));
        assertEquals(0, exec("def d = (double)0; int b = (int)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); int b = d;"));
        assertEquals(0, exec("def d = Byte.valueOf(0); int b = (int)d; b"));
        assertEquals(0, exec("def d = Short.valueOf(0); int b = (int)d; b"));
        assertEquals(0, exec("def d = Character.valueOf(0); int b = (int)d; b"));
        assertEquals(0, exec("def d = Integer.valueOf(0); int b = (int)d; b"));
        assertEquals(0, exec("def d = Long.valueOf(0); int b = (int)d; b"));
        assertEquals(0, exec("def d = Float.valueOf(0); int b = (int)d; b"));
        assertEquals(0, exec("def d = Double.valueOf(0); int b = (int)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); int b = (int)d;"));
    }

    public void testdefTolongExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; long b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; long b = (long)d;"));
        assertEquals((long)0, exec("def d = (byte)0; long b = (long)d; b"));
        assertEquals((long)0, exec("def d = (short)0; long b = (long)d; b"));
        assertEquals((long)0, exec("def d = (char)0; long b = (long)d; b"));
        assertEquals((long)0, exec("def d = 0; long b = (long)d; b"));
        assertEquals((long)0, exec("def d = (long)0; long b = (long)d; b"));
        assertEquals((long)0, exec("def d = (float)0; long b = (long)d; b"));
        assertEquals((long)0, exec("def d = (double)0; long b = (long)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); long b = d;"));
        assertEquals((long)0, exec("def d = Byte.valueOf(0); long b = (long)d; b"));
        assertEquals((long)0, exec("def d = Short.valueOf(0); long b = (long)d; b"));
        assertEquals((long)0, exec("def d = Character.valueOf(0); long b = (long)d; b"));
        assertEquals((long)0, exec("def d = Integer.valueOf(0); long b = (long)d; b"));
        assertEquals((long)0, exec("def d = Long.valueOf(0); long b = (long)d; b"));
        assertEquals((long)0, exec("def d = Float.valueOf(0); long b = (long)d; b"));
        assertEquals((long)0, exec("def d = Double.valueOf(0); long b = (long)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); long b = (long)d;"));
    }

    public void testdefTofloatExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; float b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; float b = (float)d;"));
        assertEquals((float)0, exec("def d = (byte)0; float b = (float)d; b"));
        assertEquals((float)0, exec("def d = (short)0; float b = (float)d; b"));
        assertEquals((float)0, exec("def d = (char)0; float b = (float)d; b"));
        assertEquals((float)0, exec("def d = 0; float b = (float)d; b"));
        assertEquals((float)0, exec("def d = (long)0; float b = (float)d; b"));
        assertEquals((float)0, exec("def d = (float)0; float b = (float)d; b"));
        assertEquals((float)0, exec("def d = (double)0; float b = (float)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); float b = d;"));
        assertEquals((float)0, exec("def d = Byte.valueOf(0); float b = (float)d; b"));
        assertEquals((float)0, exec("def d = Short.valueOf(0); float b = (float)d; b"));
        assertEquals((float)0, exec("def d = Character.valueOf(0); float b = (float)d; b"));
        assertEquals((float)0, exec("def d = Integer.valueOf(0); float b = (float)d; b"));
        assertEquals((float)0, exec("def d = Long.valueOf(0); float b = (float)d; b"));
        assertEquals((float)0, exec("def d = Float.valueOf(0); float b = (float)d; b"));
        assertEquals((float)0, exec("def d = Double.valueOf(0); float b = (float)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); float b = (float)d;"));
    }

    public void testdefTodoubleExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; double b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; double b = (double)d;"));
        assertEquals((double)0, exec("def d = (byte)0; double b = (double)d; b"));
        assertEquals((double)0, exec("def d = (short)0; double b = (double)d; b"));
        assertEquals((double)0, exec("def d = (char)0; double b = (double)d; b"));
        assertEquals((double)0, exec("def d = 0; double b = (double)d; b"));
        assertEquals((double)0, exec("def d = (long)0; double b = (double)d; b"));
        assertEquals((double)0, exec("def d = (float)0; double b = (double)d; b"));
        assertEquals((double)0, exec("def d = (double)0; double b = (double)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); double b = d;"));
        assertEquals((double)0, exec("def d = Byte.valueOf(0); double b = (double)d; b"));
        assertEquals((double)0, exec("def d = Short.valueOf(0); double b = (double)d; b"));
        assertEquals((double)0, exec("def d = Character.valueOf(0); double b = (double)d; b"));
        assertEquals((double)0, exec("def d = Integer.valueOf(0); double b = (double)d; b"));
        assertEquals((double)0, exec("def d = Long.valueOf(0); double b = (double)d; b"));
        assertEquals((double)0, exec("def d = Float.valueOf(0); double b = (double)d; b"));
        assertEquals((double)0, exec("def d = Double.valueOf(0); double b = (double)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); double b = (double)d;"));
    }
}
