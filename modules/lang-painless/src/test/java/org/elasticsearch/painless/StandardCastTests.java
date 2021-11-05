/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

public class StandardCastTests extends ScriptTestCase {

    public void testObjectCasts() {
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; Number n = o;"));
        exec("Object o = Integer.valueOf(0); Number n = (Number)o;");
        exec("Object o = null; Number n = (Number)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = 'string'; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; String n = o;"));
        exec("Object o = 'string'; String n = (String)o;");
        exec("Object o = null; String n = (String)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Boolean.valueOf(true); boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Boolean.valueOf(true); boolean b = (boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); byte b = (byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); short b = (short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); char b = (char)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf((int)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf((int)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); int b = (int)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); long b = (long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf(0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf(0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); float b = (float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); double b = (double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Boolean.valueOf(true); Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; Boolean b = o;"));
        exec("Object o = Boolean.valueOf(true); Boolean b = (Boolean)o;");
        exec("Object o = null; Boolean b = (Boolean)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; Byte b = o;"));
        exec("Object o = Byte.valueOf((byte)0); Byte b = (Byte)o;");
        exec("Object o = null; Byte b = (Byte)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((byte)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; Short b = o;"));
        exec("Object o = Short.valueOf((byte)0); Short b = (Short)o;");
        exec("Object o = null; Short b = (Short)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((short)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((short)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; Character b = o;"));
        exec("Object o = Character.valueOf((char)0); Character b = (Character)o;");
        exec("Object o = null; Character b = (Character)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; Integer b = o;"));
        exec("Object o = Integer.valueOf(0); Integer b = (Integer)o;");
        exec("Object o = null; Integer b = (Integer)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; Long b = o;"));
        exec("Object o = Long.valueOf((long)0); Long b = (Long)o;");
        exec("Object o = null; Long b = (Long)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((long)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; Float b = o;"));
        exec("Object o = Float.valueOf((long)0); Float b = (Float)o;");
        exec("Object o = null; Float b = (Float)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((double)0); Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Double.valueOf((long)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = null; Double b = o;"));
        exec("Object o = Double.valueOf((long)0); Double b = (Double)o;");
        exec("Object o = null; Double b = (Double)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Byte.valueOf((byte)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Short.valueOf((short)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Character.valueOf((char)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Integer.valueOf(0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Long.valueOf((long)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Object o = Float.valueOf((float)0); Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Object o = new ArrayList(); ArrayList b = o;"));
        exec("Object o = new ArrayList(); ArrayList b = (ArrayList)o;");
    }

    public void testNumberCasts() {
        exec("Number o = Integer.valueOf(0); Object n = o;");
        exec("Number o = null; Object n = o;");
        exec("Number o = Integer.valueOf(0); Object n = (Object)o;");
        exec("Number o = null; Object n = (Object)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = 'string'; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = 'string'; String n = (String)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Boolean.valueOf(true); boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Boolean.valueOf(true); boolean b = (boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); byte b = (byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); short b = (short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); char b = (char)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf((int)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf((int)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); int b = (int)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); long b = (long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf(0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf(0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); float b = (float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); double b = (double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Boolean.valueOf(true); Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Boolean.valueOf(true); Boolean b = (Boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; Byte b = o;"));
        exec("Number o = Byte.valueOf((byte)0); Byte b = (Byte)o;");
        exec("Number o = null; Byte b = (Byte)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((byte)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; Short b = o;"));
        exec("Number o = Short.valueOf((byte)0); Short b = (Short)o;");
        exec("Number o = null; Short b = (Short)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((short)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((short)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; Integer b = o;"));
        exec("Number o = Integer.valueOf(0); Integer b = (Integer)o;");
        exec("Number o = null; Integer b = (Integer)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; Long b = o;"));
        exec("Number o = Long.valueOf((long)0); Long b = (Long)o;");
        exec("Number o = null; Long b = (Long)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((long)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; Float b = o;"));
        exec("Number o = Float.valueOf((long)0); Float b = (Float)o;");
        exec("Number o = null; Float b = (Float)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((double)0); Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Double.valueOf((long)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = null; Double b = o;"));
        exec("Number o = Double.valueOf((long)0); Double b = (Double)o;");
        exec("Number o = null; Double b = (Double)o;");
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Byte.valueOf((byte)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Short.valueOf((short)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Character.valueOf((char)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Integer.valueOf(0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Long.valueOf((long)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = Float.valueOf((float)0); Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Number o = new ArrayList(); ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Number o = new ArrayList(); ArrayList b = (ArrayList)o;"));
    }

    public void testStringCasts() {
        exec("String o = 'string'; Object n = o;");
        exec("String o = null; Object n = o;");
        exec("String o = 'string'; Object n = (Object)o;");
        exec("String o = null; Object n = (Object)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Number n = (String)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Number n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; boolean b = (boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; byte b = (byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; short b = (short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String s = 's'; char c = s; return c"));
        assertEquals('s', exec("String s = 's'; char c = (char)s; return c"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; char b = (char)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; int b = (int)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; long b = (long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; float b = (float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; double b = (double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Boolean b = (Boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = null; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("String o = 'string'; ArrayList b = (ArrayList)o;"));
    }

    public void testPrimitiveBooleanCasts() {
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Object n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Object n = (Object)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Number n = (boolean)o;"));

        exec("boolean o = true; boolean b = o;");
        exec("boolean o = true; boolean b = (boolean)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; byte b = (byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; short b = (short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; char b = (char)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; int b = (int)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; long b = (long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; float b = (float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; double b = (double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("boolean o = true; ArrayList b = (ArrayList)o;"));
    }

    public void testBoxedBooleanCasts() {
        exec("Boolean o = Boolean.valueOf(true); Object n = o;");
        exec("Boolean o = null; Object n = o;");
        exec("Boolean o = Boolean.valueOf(true); Object n = (Object)o;");
        exec("Boolean o = null; Object n = (Object)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Number n = (Boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Number n = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); boolean b = (boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; byte b = (byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; short b = (short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; char b = (char)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; int b = (int)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; long b = (long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; float b = (float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; double b = (double)o;"));

        exec("Boolean o = Boolean.valueOf(true); Boolean b = o;");
        exec("Boolean o = null; Boolean b = o;");
        exec("Boolean o = Boolean.valueOf(true); Boolean b = (Boolean)o;");
        exec("Boolean o = null; Boolean b = (Boolean)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = null; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Boolean o = Boolean.valueOf(true); ArrayList b = (ArrayList)o;"));
    }

    public void testPrimitiveByteCasts() {
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Object n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Object n = (Object)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Number n = (Number)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; boolean b = (boolean)o;"));

        exec("byte o = 0; byte b = o;");
        exec("byte o = 0; byte b = (byte)o;");

        exec("byte o = 0; short b = o;");
        exec("byte o = 0; short b = (short)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; char b = o;"));
        exec("byte o = 0; char b = (char)o;");

        exec("byte o = 0; int b = o;");
        exec("byte o = 0; int b = (int)o;");

        exec("byte o = 0; long b = o;");
        exec("byte o = 0; long b = (long)o;");

        exec("byte o = 0; float b = o;");
        exec("byte o = 0; float b = (float)o;");

        exec("byte o = 0; double b = o;");
        exec("byte o = 0; double b = (double)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("byte o = 0; ArrayList b = (ArrayList)o;"));
    }

    public void testByteCasts() {
        exec("Byte o = Byte.valueOf((byte)0); Object n = o;");
        exec("Byte o = null; Object n = o;");
        exec("Byte o = Byte.valueOf((byte)0); Object n = (Object)o;");
        exec("Byte o = null; Object n = (Object)o;");

        exec("Byte o = Byte.valueOf((byte)0); Number n = o;");
        exec("Byte o = null; Number n = o;");
        exec("Byte o = Byte.valueOf((byte)0); Number n = (Number)o;");
        exec("Byte o = null; Number n = (Number)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); String n = (String)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); boolean b = (boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; byte b = (byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; short b = (short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; char b = (char)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; int b = (int)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; long b = (long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; float b = (float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; double b = (double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Boolean b = (Boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Boolean b = (Boolean)o;"));

        exec("Byte o = Byte.valueOf((byte)0); Byte b = o;");
        exec("Byte o = null; Byte b = o;");
        exec("Byte o = Byte.valueOf((byte)0); Byte b = (Byte)o;");
        exec("Byte o = null; Byte b = (Byte)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = null; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Byte o = Byte.valueOf((byte)0); ArrayList b = (ArrayList)o;"));
    }

    public void testPrimitiveShortCasts() {
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Object n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Object n = (Object)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Number n = (Number)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; byte b = o;"));
        exec("short o = 0; byte b = (byte)o;");

        exec("short o = 0; short b = o;");
        exec("short o = 0; short b = (short)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; char b = o;"));
        exec("short o = 0; char b = (char)o;");

        exec("short o = 0; int b = o;");
        exec("short o = 0; int b = (int)o;");

        exec("short o = 0; long b = o;");
        exec("short o = 0; long b = (long)o;");

        exec("short o = 0; float b = o;");
        exec("short o = 0; float b = (float)o;");

        exec("short o = 0; double b = o;");
        exec("short o = 0; double b = (double)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("short o = 0; ArrayList b = (ArrayList)o;"));
    }

    public void testShortCasts() {
        exec("Short o = Short.valueOf((short)0); Object n = o;");
        exec("Short o = null; Object n = o;");
        exec("Short o = Short.valueOf((short)0); Object n = (Object)o;");
        exec("Short o = null; Object n = (Object)o;");

        exec("Short o = Short.valueOf((short)0); Number n = o;");
        exec("Short o = null; Number n = o;");
        exec("Short o = Short.valueOf((short)0); Number n = (Number)o;");
        exec("Short o = null; Number n = (Number)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); String n = (String)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); boolean b = (boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; byte b = (byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; short b = (short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; char b = (char)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; int b = (int)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; long b = (long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; float b = (float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; double b = (double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Boolean b = (Boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Byte b = (Byte)o;"));

        exec("Short o = Short.valueOf((short)0); Short b = o;");
        exec("Short o = null; Short b = o;");
        exec("Short o = Short.valueOf((short)0); Short b = (Short)o;");
        exec("Short o = null; Short b = (Short)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = null; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Short o = Short.valueOf((short)0); ArrayList b = (ArrayList)o;"));
    }

    public void testPrimitiveCharCasts() {
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Object n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Object n = (Object)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Number n = (Number)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; String n = o;"));
        exec("char o = 0; String n = (String)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; byte b = o;"));
        exec("char o = 0; byte b = (byte)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; short b = o;"));
        exec("char o = 0; short b = (short)o;");

        exec("char o = 0; char b = o;");
        exec("char o = 0; char b = (char)o;");

        exec("char o = 0; int b = o;");
        exec("char o = 0; int b = (int)o;");

        exec("char o = 0; long b = o;");
        exec("char o = 0; long b = (long)o;");

        exec("char o = 0; float b = o;");
        exec("char o = 0; float b = (float)o;");

        exec("char o = 0; double b = o;");
        exec("char o = 0; double b = (double)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("char o = 0; ArrayList b = (ArrayList)o;"));
    }

    public void testCharacterCasts() {
        exec("Character o = Character.valueOf((char)0); Object n = o;");
        exec("Character o = null; Object n = o;");
        exec("Character o = Character.valueOf((char)0); Object n = (Object)o;");
        exec("Character o = null; Object n = (Object)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Number n = (Number)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Number n = (Number)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); String n = (String)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); boolean b = (boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; byte b = (byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; short b = (short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; char b = (char)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; int b = (int)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; long b = (long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; float b = (float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; double b = (double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Boolean b = (Boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Short b = (Short)o;"));

        exec("Character o = Character.valueOf((char)0); Character b = o;");
        exec("Character o = null; Character b = o;");
        exec("Character o = Character.valueOf((char)0); Character b = (Character)o;");
        exec("Character o = null; Character b = (Character)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = null; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Character o = Character.valueOf((char)0); ArrayList b = (ArrayList)o;"));
    }

    public void testPrimitiveIntCasts() {
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Object n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Object n = (Object)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Number n = (Number)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; byte b = o;"));
        exec("int o = 0; byte b = (byte)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; short b = o;"));
        exec("int o = 0; short b = (short)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; char b = o;"));
        exec("int o = 0; char b = (char)o;");

        exec("int o = 0; int b = o;");
        exec("int o = 0; int b = (int)o;");

        exec("int o = 0; long b = o;");
        exec("int o = 0; long b = (long)o;");

        exec("int o = 0; float b = o;");
        exec("int o = 0; float b = (float)o;");

        exec("int o = 0; double b = o;");
        exec("int o = 0; double b = (double)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("int o = 0; ArrayList b = (ArrayList)o;"));
    }

    public void testIntegerCasts() {
        exec("Integer o = Integer.valueOf((int)0); Object n = o;");
        exec("Integer o = null; Object n = o;");
        exec("Integer o = Integer.valueOf((int)0); Object n = (Object)o;");
        exec("Integer o = null; Object n = (Object)o;");

        exec("Integer o = Integer.valueOf((int)0); Number n = o;");
        exec("Integer o = null; Number n = o;");
        exec("Integer o = Integer.valueOf((int)0); Number n = (Number)o;");
        exec("Integer o = null; Number n = (Number)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); String n = (String)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); boolean b = (boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; byte b = (byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; short b = (short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; char b = (char)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; int b = (int)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; long b = (long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; float b = (float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; double b = (double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Boolean b = (Boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Character b = (Character)o;"));

        exec("Integer o = Integer.valueOf((int)0); Integer b = o;");
        exec("Integer o = null; Integer b = o;");
        exec("Integer o = Integer.valueOf((int)0); Integer b = (Integer)o;");
        exec("Integer o = null; Integer b = (Integer)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = null; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Integer o = Integer.valueOf((int)0); ArrayList b = (ArrayList)o;"));
    }

    public void testPrimitiveLongCasts() {
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Object n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Object n = (Object)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Number n = (Number)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; byte b = o;"));
        exec("long o = 0; byte b = (byte)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; short b = o;"));
        exec("long o = 0; short b = (short)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; char b = o;"));
        exec("long o = 0; char b = (char)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; int b = o;"));
        exec("long o = 0; int b = (int)o;");

        exec("long o = 0; long b = o;");
        exec("long o = 0; long b = (long)o;");

        exec("long o = 0; float b = o;");
        exec("long o = 0; float b = (float)o;");

        exec("long o = 0; double b = o;");
        exec("long o = 0; double b = (double)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("long o = 0; ArrayList b = (ArrayList)o;"));
    }

    public void testLongCasts() {
        exec("Long o = Long.valueOf((long)0); Object n = o;");
        exec("Long o = null; Object n = o;");
        exec("Long o = Long.valueOf((long)0); Object n = (Object)o;");
        exec("Long o = null; Object n = (Object)o;");

        exec("Long o = Long.valueOf((long)0); Number n = o;");
        exec("Long o = null; Number n = o;");
        exec("Long o = Long.valueOf((long)0); Number n = (Number)o;");
        exec("Long o = null; Number n = (Number)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); String n = (String)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); boolean b = (boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; byte b = (byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; short b = (short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; char b = (char)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; int b = (int)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; long b = (long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; float b = (float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; double b = (double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Boolean b = (Boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Integer b = (Integer)o;"));

        exec("Long o = Long.valueOf((long)0); Long b = o;");
        exec("Long o = null; Long b = o;");
        exec("Long o = Long.valueOf((long)0); Long b = (Long)o;");
        exec("Long o = null; Long b = (Long)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = null; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Long o = Long.valueOf((long)0); ArrayList b = (ArrayList)o;"));
    }

    public void testPrimitiveFloatCasts() {
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Object n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Object n = (Object)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Number n = (Number)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; byte b = o;"));
        exec("float o = 0; byte b = (byte)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; short b = o;"));
        exec("float o = 0; short b = (short)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; char b = o;"));
        exec("float o = 0; char b = (char)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; int b = o;"));
        exec("float o = 0; int b = (int)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; long b = o;"));
        exec("float o = 0; long b = (long)o;");

        exec("float o = 0; float b = o;");
        exec("float o = 0; float b = (float)o;");

        exec("float o = 0; double b = o;");
        exec("float o = 0; double b = (double)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("float o = 0; ArrayList b = (ArrayList)o;"));
    }

    public void testFloatCasts() {
        exec("Float o = Float.valueOf((float)0); Object n = o;");
        exec("Float o = null; Object n = o;");
        exec("Float o = Float.valueOf((float)0); Object n = (Object)o;");
        exec("Float o = null; Object n = (Object)o;");

        exec("Float o = Float.valueOf((float)0); Number n = o;");
        exec("Float o = null; Number n = o;");
        exec("Float o = Float.valueOf((float)0); Number n = (Number)o;");
        exec("Float o = null; Number n = (Number)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); String n = (String)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); boolean b = (boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; byte b = (byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; short b = (short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; char b = (char)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; int b = (int)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; long b = (long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; float b = (float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; double b = (double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Boolean b = (Boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Long b = (Long)o;"));

        exec("Float o = Float.valueOf((float)0); Float b = o;");
        exec("Float o = null; Float b = o;");
        exec("Float o = Float.valueOf((float)0); Float b = (Float)o;");
        exec("Float o = null; Float b = (Float)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); Double b = (Double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = null; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Float o = Float.valueOf((float)0); ArrayList b = (ArrayList)o;"));
    }

    public void testPrimitiveDoubleCasts() {
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Object n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Object n = (Object)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Number n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Number n = (Number)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; byte b = o;"));
        exec("double o = 0; byte b = (byte)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; short b = o;"));
        exec("double o = 0; short b = (short)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; char b = o;"));
        exec("double o = 0; char b = (char)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; int b = o;"));
        exec("double o = 0; int b = (int)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; long b = o;"));
        exec("double o = 0; long b = (long)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; float b = o;"));
        exec("double o = 0; float b = (float)o;");

        exec("double o = 0; double b = o;");
        exec("double o = 0; double b = (double)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Float b = (Float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; Double b = (Double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("double o = 0; ArrayList b = (ArrayList)o;"));
    }

    public void testDoubleCasts() {
        exec("Double o = Double.valueOf((double)0); Object n = o;");
        exec("Double o = null; Object n = o;");
        exec("Double o = Double.valueOf((double)0); Object n = (Object)o;");
        exec("Double o = null; Object n = (Object)o;");

        exec("Double o = Double.valueOf((double)0); Number n = o;");
        exec("Double o = null; Number n = o;");
        exec("Double o = Double.valueOf((double)0); Number n = (Number)o;");
        exec("Double o = null; Number n = (Number)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; String n = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); String n = (String)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; String n = (String)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); boolean b = (boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; boolean b = (boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); byte b = (byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; byte b = (byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); short b = (short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; short b = (short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; char b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); char b = (char)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; char b = (char)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; int b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); int b = (int)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; int b = (int)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); long b = (long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; long b = (long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); float b = (float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; float b = (float)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; double b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); double b = (double)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; double b = (double)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Boolean b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Boolean b = (Boolean)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Boolean b = (Boolean)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Byte b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Byte b = (Byte)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Byte b = (Byte)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Short b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Short b = (Short)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Short b = (Short)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Character b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Character b = (Character)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Character b = (Character)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Integer b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Integer b = (Integer)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Integer b = (Integer)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Long b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Long b = (Long)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Long b = (Long)o;"));

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Float b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); Float b = (Float)o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = null; Float b = (Float)o;"));

        exec("Double o = Double.valueOf((double)0); Double b = o;");
        exec("Double o = null; Double b = o;");
        exec("Double o = Double.valueOf((double)0); Double b = (Double)o;");
        exec("Double o = null; Double b = (Double)o;");

        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); ArrayList b = o;"));
        expectScriptThrows(ClassCastException.class, () -> exec("Double o = Double.valueOf((double)0); ArrayList b = (ArrayList)o;"));
    }
}
