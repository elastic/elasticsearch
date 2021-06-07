/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 's'; char b = d;"));
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
        assertEquals('s', exec("def d = 's'; char b = (char)d; b"));
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

    public void testdefToBooleanImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Boolean b = d;"));
        assertEquals(true, exec("def d = true; Boolean b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (byte)0; Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (short)0; Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (char)0; Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (int)0; Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (long)0; Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; Boolean b = d;"));
        assertEquals(false, exec("def d = Boolean.valueOf(false); Boolean b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Byte.valueOf(0); Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Short.valueOf(0); Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Character.valueOf(0); Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Integer.valueOf(0); Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Long.valueOf(0); Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); Boolean b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Boolean b = d;"));
    }

    public void testdefToByteImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Byte b = d;"));
        assertEquals((byte)0, exec("def d = (byte)0; Byte b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (short)0; Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (char)0; Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (int)0; Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (long)0; Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Byte b = d;"));
        assertEquals((byte)0, exec("def d = Byte.valueOf(0); Byte b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Short.valueOf(0); Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Character.valueOf(0); Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Integer.valueOf(0); Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Long.valueOf(0); Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); Byte b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Byte b = d;"));
    }

    public void testdefToShortImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Short b = d;"));
        assertEquals((short)0, exec("def d = (byte)0; Short b = d; b"));
        assertEquals((short)0, exec("def d = (short)0; Short b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (char)0; Short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (int)0; Short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (long)0; Short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; Short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; Short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Short b = d;"));
        assertEquals((short)0, exec("def d = Byte.valueOf(0); Short b = d; b"));
        assertEquals((short)0, exec("def d = Short.valueOf(0); Short b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Character.valueOf(0); Short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Integer.valueOf(0); Short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Long.valueOf(0); Short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); Short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); Short b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Short b = d;"));
    }

    public void testdefToCharacterImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 's'; Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (byte)0; Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (short)0; Character b = d;"));
        assertEquals((char)0, exec("def d = (char)0; Character b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (int)0; Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (long)0; Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Byte.valueOf(0); Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Short.valueOf(0); Character b = d;"));
        assertEquals((char)0, exec("def d = Character.valueOf(0); Character b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Integer.valueOf(0); Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Long.valueOf(0); Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); Character b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Character b = d;"));
    }

    public void testdefToIntegerImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Integer b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Integer b = d;"));
        assertEquals(0, exec("def d = (byte)0; Integer b = d; b"));
        assertEquals(0, exec("def d = (short)0; Integer b = d; b"));
        assertEquals(0, exec("def d = (char)0; Integer b = d; b"));
        assertEquals(0, exec("def d = 0; Integer b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (long)0; Integer b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; Integer b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; Integer b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Integer b = d;"));
        assertEquals(0, exec("def d = Byte.valueOf(0); Integer b = d; b"));
        assertEquals(0, exec("def d = Short.valueOf(0); Integer b = d; b"));
        assertEquals(0, exec("def d = Character.valueOf(0); Integer b = d; b"));
        assertEquals(0, exec("def d = Integer.valueOf(0); Integer b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Long.valueOf(0); Integer b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); Integer b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); Integer b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Integer b = d;"));
    }

    public void testdefToLongImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Long b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Long b = d;"));
        assertEquals((long)0, exec("def d = (byte)0; Long b = d; b"));
        assertEquals((long)0, exec("def d = (short)0; Long b = d; b"));
        assertEquals((long)0, exec("def d = (char)0; Long b = d; b"));
        assertEquals((long)0, exec("def d = 0; Long b = d; b"));
        assertEquals((long)0, exec("def d = (long)0; Long b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; Long b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; Long b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Long b = d;"));
        assertEquals((long)0, exec("def d = Byte.valueOf(0); Long b = d; b"));
        assertEquals((long)0, exec("def d = Short.valueOf(0); Long b = d; b"));
        assertEquals((long)0, exec("def d = Character.valueOf(0); Long b = d; b"));
        assertEquals((long)0, exec("def d = Integer.valueOf(0); Long b = d; b"));
        assertEquals((long)0, exec("def d = Long.valueOf(0); Long b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); Long b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); Long b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Long b = d;"));
    }

    public void testdefToFloatImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Float b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Float b = d;"));
        assertEquals((float)0, exec("def d = (byte)0; Float b = d; b"));
        assertEquals((float)0, exec("def d = (short)0; Float b = d; b"));
        assertEquals((float)0, exec("def d = (char)0; Float b = d; b"));
        assertEquals((float)0, exec("def d = 0; Float b = d; b"));
        assertEquals((float)0, exec("def d = (long)0; Float b = d; b"));
        assertEquals((float)0, exec("def d = (float)0; Float b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; Float b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Float b = d;"));
        assertEquals((float)0, exec("def d = Byte.valueOf(0); Float b = d; b"));
        assertEquals((float)0, exec("def d = Short.valueOf(0); Float b = d; b"));
        assertEquals((float)0, exec("def d = Character.valueOf(0); Float b = d; b"));
        assertEquals((float)0, exec("def d = Integer.valueOf(0); Float b = d; b"));
        assertEquals((float)0, exec("def d = Long.valueOf(0); Float b = d; b"));
        assertEquals((float)0, exec("def d = Float.valueOf(0); Float b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); Float b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Float b = d;"));
    }

    public void testdefToDoubleImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Double b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Double b = d;"));
        assertEquals((double)0, exec("def d = (byte)0; Double b = d; b"));
        assertEquals((double)0, exec("def d = (short)0; Double b = d; b"));
        assertEquals((double)0, exec("def d = (char)0; Double b = d; b"));
        assertEquals((double)0, exec("def d = 0; Double b = d; b"));
        assertEquals((double)0, exec("def d = (long)0; Double b = d; b"));
        assertEquals((double)0, exec("def d = (float)0; Double b = d; b"));
        assertEquals((double)0, exec("def d = (double)0; Double b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Double b = d;"));
        assertEquals((double)0, exec("def d = Byte.valueOf(0); Double b = d; b"));
        assertEquals((double)0, exec("def d = Short.valueOf(0); Double b = d; b"));
        assertEquals((double)0, exec("def d = Character.valueOf(0); Double b = d; b"));
        assertEquals((double)0, exec("def d = Integer.valueOf(0); Double b = d; b"));
        assertEquals((double)0, exec("def d = Long.valueOf(0); Double b = d; b"));
        assertEquals((double)0, exec("def d = Float.valueOf(0); Double b = d; b"));
        assertEquals((double)0, exec("def d = Double.valueOf(0); Double b = d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Double b = d;"));
    }

    public void testdefToBooleanExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Boolean b = (Boolean)d;"));
        assertEquals(true, exec("def d = true; Boolean b = (Boolean)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (byte)0; Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (short)0; Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (char)0; Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (int)0; Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (long)0; Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (float)0; Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (double)0; Boolean b = (Boolean)d;"));
        assertEquals(false, exec("def d = Boolean.valueOf(false); Boolean b = (Boolean)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Byte.valueOf(0); Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Short.valueOf(0); Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Character.valueOf(0); Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Integer.valueOf(0); Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Long.valueOf(0); Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Float.valueOf(0); Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Double.valueOf(0); Boolean b = (Boolean)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Boolean b = (Boolean)d;"));
    }

    public void testdefToByteExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Byte b = (Byte)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Byte b = (Byte)d;"));
        assertEquals((byte)0, exec("def d = (byte)0; Byte b = (Byte)d; b"));
        assertEquals((byte)0, exec("def d = (short)0; Byte b = (Byte)d; b"));
        assertEquals((byte)0, exec("def d = (char)0; Byte b = (Byte)d; b"));
        assertEquals((byte)0, exec("def d = 0; Byte b = (Byte)d; b"));
        assertEquals((byte)0, exec("def d = (long)0; Byte b = (Byte)d; b"));
        assertEquals((byte)0, exec("def d = (float)0; Byte b = (Byte)d; b"));
        assertEquals((byte)0, exec("def d = (double)0; Byte b = (Byte)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Byte b = d;"));
        assertEquals((byte)0, exec("def d = Byte.valueOf(0); Byte b = (Byte)d; b"));
        assertEquals((byte)0, exec("def d = Short.valueOf(0); Byte b = (Byte)d; b"));
        assertEquals((byte)0, exec("def d = Character.valueOf(0); Byte b = (Byte)d; b"));
        assertEquals((byte)0, exec("def d = Integer.valueOf(0); Byte b = (Byte)d; b"));
        assertEquals((byte)0, exec("def d = Long.valueOf(0); Byte b = (Byte)d; b"));
        assertEquals((byte)0, exec("def d = Float.valueOf(0); Byte b = (Byte)d; b"));
        assertEquals((byte)0, exec("def d = Double.valueOf(0); Byte b = (Byte)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Byte b = (Byte)d;"));
    }

    public void testdefToShortExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Short b = (Short)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Short b = (Short)d;"));
        assertEquals((short)0, exec("def d = (byte)0; Short b = (Short)d; b"));
        assertEquals((short)0, exec("def d = (short)0; Short b = (Short)d; b"));
        assertEquals((short)0, exec("def d = (char)0; Short b = (Short)d; b"));
        assertEquals((short)0, exec("def d = 0; Short b = (Short)d; b"));
        assertEquals((short)0, exec("def d = (long)0; Short b = (Short)d; b"));
        assertEquals((short)0, exec("def d = (float)0; Short b = (Short)d; b"));
        assertEquals((short)0, exec("def d = (double)0; Short b = (Short)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Short b = d;"));
        assertEquals((short)0, exec("def d = Byte.valueOf(0); Short b = (Short)d; b"));
        assertEquals((short)0, exec("def d = Short.valueOf(0); Short b = (Short)d; b"));
        assertEquals((short)0, exec("def d = Character.valueOf(0); Short b = (Short)d; b"));
        assertEquals((short)0, exec("def d = Integer.valueOf(0); Short b = (Short)d; b"));
        assertEquals((short)0, exec("def d = Long.valueOf(0); Short b = (Short)d; b"));
        assertEquals((short)0, exec("def d = Float.valueOf(0); Short b = (Short)d; b"));
        assertEquals((short)0, exec("def d = Double.valueOf(0); Short b = (Short)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Short b = (Short)d;"));
    }

    public void testdefToCharacterExplicit() {
        assertEquals('s', exec("def d = 's'; Character b = (Character)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Character b = (Character)d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Character b = (Character)d;"));
        assertEquals((char)0, exec("def d = (byte)0; Character b = (Character)d; b"));
        assertEquals((char)0, exec("def d = (short)0; Character b = (Character)d; b"));
        assertEquals((char)0, exec("def d = (char)0; Character b = (Character)d; b"));
        assertEquals((char)0, exec("def d = 0; Character b = (Character)d; b"));
        assertEquals((char)0, exec("def d = (long)0; Character b = (Character)d; b"));
        assertEquals((char)0, exec("def d = (float)0; Character b = (Character)d; b"));
        assertEquals((char)0, exec("def d = (double)0; Character b = (Character)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Character b = d;"));
        assertEquals((char)0, exec("def d = Byte.valueOf(0); Character b = (Character)d; b"));
        assertEquals((char)0, exec("def d = Short.valueOf(0); Character b = (Character)d; b"));
        assertEquals((char)0, exec("def d = Character.valueOf(0); Character b = (Character)d; b"));
        assertEquals((char)0, exec("def d = Integer.valueOf(0); Character b = (Character)d; b"));
        assertEquals((char)0, exec("def d = Long.valueOf(0); Character b = (Character)d; b"));
        assertEquals((char)0, exec("def d = Float.valueOf(0); Character b = (Character)d; b"));
        assertEquals((char)0, exec("def d = Double.valueOf(0); Character b = (Character)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Character b = (Character)d;"));
    }

    public void testdefToIntegerExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Integer b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Integer b = (Integer)d;"));
        assertEquals(0, exec("def d = (byte)0; Integer b = (Integer)d; b"));
        assertEquals(0, exec("def d = (short)0; Integer b = (Integer)d; b"));
        assertEquals(0, exec("def d = (char)0; Integer b = (Integer)d; b"));
        assertEquals(0, exec("def d = 0; Integer b = (Integer)d; b"));
        assertEquals(0, exec("def d = (long)0; Integer b = (Integer)d; b"));
        assertEquals(0, exec("def d = (float)0; Integer b = (Integer)d; b"));
        assertEquals(0, exec("def d = (double)0; Integer b = (Integer)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Integer b = d;"));
        assertEquals(0, exec("def d = Byte.valueOf(0); Integer b = (Integer)d; b"));
        assertEquals(0, exec("def d = Short.valueOf(0); Integer b = (Integer)d; b"));
        assertEquals(0, exec("def d = Character.valueOf(0); Integer b = (Integer)d; b"));
        assertEquals(0, exec("def d = Integer.valueOf(0); Integer b = (Integer)d; b"));
        assertEquals(0, exec("def d = Long.valueOf(0); Integer b = (Integer)d; b"));
        assertEquals(0, exec("def d = Float.valueOf(0); Integer b = (Integer)d; b"));
        assertEquals(0, exec("def d = Double.valueOf(0); Integer b = (Integer)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Integer b = (Integer)d;"));
    }

    public void testdefToLongExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Long b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Long b = (Long)d;"));
        assertEquals((long)0, exec("def d = (byte)0; Long b = (Long)d; b"));
        assertEquals((long)0, exec("def d = (short)0; Long b = (Long)d; b"));
        assertEquals((long)0, exec("def d = (char)0; Long b = (Long)d; b"));
        assertEquals((long)0, exec("def d = 0; Long b = (Long)d; b"));
        assertEquals((long)0, exec("def d = (long)0; Long b = (Long)d; b"));
        assertEquals((long)0, exec("def d = (float)0; Long b = (Long)d; b"));
        assertEquals((long)0, exec("def d = (double)0; Long b = (Long)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Long b = d;"));
        assertEquals((long)0, exec("def d = Byte.valueOf(0); Long b = (Long)d; b"));
        assertEquals((long)0, exec("def d = Short.valueOf(0); Long b = (Long)d; b"));
        assertEquals((long)0, exec("def d = Character.valueOf(0); Long b = (Long)d; b"));
        assertEquals((long)0, exec("def d = Integer.valueOf(0); Long b = (Long)d; b"));
        assertEquals((long)0, exec("def d = Long.valueOf(0); Long b = (Long)d; b"));
        assertEquals((long)0, exec("def d = Float.valueOf(0); Long b = (Long)d; b"));
        assertEquals((long)0, exec("def d = Double.valueOf(0); Long b = (Long)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Long b = (Long)d;"));
    }

    public void testdefToFloatExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Float b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Float b = (Float)d;"));
        assertEquals((float)0, exec("def d = (byte)0; Float b = (Float)d; b"));
        assertEquals((float)0, exec("def d = (short)0; Float b = (Float)d; b"));
        assertEquals((float)0, exec("def d = (char)0; Float b = (Float)d; b"));
        assertEquals((float)0, exec("def d = 0; Float b = (Float)d; b"));
        assertEquals((float)0, exec("def d = (long)0; Float b = (Float)d; b"));
        assertEquals((float)0, exec("def d = (float)0; Float b = (Float)d; b"));
        assertEquals((float)0, exec("def d = (double)0; Float b = (Float)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Float b = d;"));
        assertEquals((float)0, exec("def d = Byte.valueOf(0); Float b = (Float)d; b"));
        assertEquals((float)0, exec("def d = Short.valueOf(0); Float b = (Float)d; b"));
        assertEquals((float)0, exec("def d = Character.valueOf(0); Float b = (Float)d; b"));
        assertEquals((float)0, exec("def d = Integer.valueOf(0); Float b = (Float)d; b"));
        assertEquals((float)0, exec("def d = Long.valueOf(0); Float b = (Float)d; b"));
        assertEquals((float)0, exec("def d = Float.valueOf(0); Float b = (Float)d; b"));
        assertEquals((float)0, exec("def d = Double.valueOf(0); Float b = (Float)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Float b = (Float)d;"));
    }

    public void testdefToDoubleExplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 'string'; Double b = d;"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = true; Double b = (Double)d;"));
        assertEquals((double)0, exec("def d = (byte)0; Double b = (Double)d; b"));
        assertEquals((double)0, exec("def d = (short)0; Double b = (Double)d; b"));
        assertEquals((double)0, exec("def d = (char)0; Double b = (Double)d; b"));
        assertEquals((double)0, exec("def d = 0; Double b = (Double)d; b"));
        assertEquals((double)0, exec("def d = (long)0; Double b = (Double)d; b"));
        assertEquals((double)0, exec("def d = (float)0; Double b = (Double)d; b"));
        assertEquals((double)0, exec("def d = (double)0; Double b = (Double)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = Boolean.valueOf(true); Double b = d;"));
        assertEquals((double)0, exec("def d = Byte.valueOf(0); Double b = (Double)d; b"));
        assertEquals((double)0, exec("def d = Short.valueOf(0); Double b = (Double)d; b"));
        assertEquals((double)0, exec("def d = Character.valueOf(0); Double b = (Double)d; b"));
        assertEquals((double)0, exec("def d = Integer.valueOf(0); Double b = (Double)d; b"));
        assertEquals((double)0, exec("def d = Long.valueOf(0); Double b = (Double)d; b"));
        assertEquals((double)0, exec("def d = Float.valueOf(0); Double b = (Double)d; b"));
        assertEquals((double)0, exec("def d = Double.valueOf(0); Double b = (Double)d; b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = new ArrayList(); Double b = (Double)d;"));
    }

    public void testdefToStringImplicit() {
        expectScriptThrows(ClassCastException.class, () -> exec("def d = (char)'s'; String b = d;"));
    }

    public void testdefToStringExplicit() {
        assertEquals("s", exec("def d = (char)'s'; String b = (String)d; b"));
    }

    public void testConstFoldingDefCast() {
        assertFalse((boolean)exec("def chr = 10; return (chr == (char)'x');"));
        assertFalse((boolean)exec("def chr = 10; return (chr >= (char)'x');"));
        assertTrue((boolean)exec("def chr = (char)10; return (chr <= (char)'x');"));
        assertTrue((boolean)exec("def chr = 10; return (chr < (char)'x');"));
        assertFalse((boolean)exec("def chr = (char)10; return (chr > (char)'x');"));
        assertFalse((boolean)exec("def chr = 10L; return (chr > (char)'x');"));
        assertFalse((boolean)exec("def chr = 10F; return (chr > (char)'x');"));
        assertFalse((boolean)exec("def chr = 10D; return (chr > (char)'x');"));
        assertFalse((boolean)exec("def chr = (char)10L; return (chr > (byte)10);"));
        assertFalse((boolean)exec("def chr = (char)10L; return (chr > (double)(byte)(char)10);"));
    }

    // TODO: remove this when the transition from Joda to Java datetimes is completed
    public void testdefToZonedDateTime() {
        assertEquals(0L, exec(
                "Instant instant = Instant.ofEpochMilli(434931330000L);" +
                "def d = new JodaCompatibleZonedDateTime(instant, ZoneId.of('Z'));" +
                "def x = new HashMap(); x.put('dt', d);" +
                "ZonedDateTime t = x['dt'];" +
                "def y = t;" +
                "t = y;" +
                "return ChronoUnit.MILLIS.between(d, t);"
        ));
    }
}
