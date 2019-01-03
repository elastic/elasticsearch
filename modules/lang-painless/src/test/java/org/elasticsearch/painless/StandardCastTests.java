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

        assertEquals('s', exec("String s = 's'; (char)s"));
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
}
