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

public class BoxedCastTests extends ScriptTestCase {

    public void testMethodCallByteToBoxedCasts() {
        assertEquals(0, exec("byte u = 1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        assertEquals(0, exec("byte u = 1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("byte u = 1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("byte u = 1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("byte u = 1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("byte u = 1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("byte u = 1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        assertEquals(0, exec("Byte u = Byte.valueOf((byte)1); Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        assertEquals(0, exec("Byte u = Byte.valueOf((byte)1); Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Byte u = Byte.valueOf((byte)1); Character b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("Byte u = Byte.valueOf((byte)1); Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("Byte u = Byte.valueOf((byte)1); Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("Byte u = Byte.valueOf((byte)1); Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Byte u = Byte.valueOf((byte)1); Double b = Double.valueOf((double)1); b.compareTo(u);"));

        assertEquals(0, exec("byte u = 1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        assertEquals(0, exec("byte u = 1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("byte u = 1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("byte u = 1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("byte u = 1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("byte u = 1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("byte u = 1; def b = Double.valueOf((double)1); b.compareTo(u);"));

        assertEquals(0, exec("Byte u = Byte.valueOf((byte)1); def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        assertEquals(0, exec("Byte u = Byte.valueOf((byte)1); def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Byte u = Byte.valueOf((byte)1); def b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("Byte u = Byte.valueOf((byte)1); def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("Byte u = Byte.valueOf((byte)1); def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("Byte u = Byte.valueOf((byte)1); def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Byte u = Byte.valueOf((byte)1); def b = Double.valueOf((double)1); b.compareTo(u);"));

        assertEquals(0, exec("def u = (byte)1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (byte)1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (byte)1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (byte)1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (byte)1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (byte)1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (byte)1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        assertEquals(0, exec("def u = (byte)1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (byte)1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (byte)1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (byte)1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (byte)1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (byte)1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (byte)1; def b = Double.valueOf((double)1); b.compareTo(u);"));
    }

    public void testMethodCallShortToBoxedCasts() {
        expectScriptThrows(ClassCastException.class,
                () -> exec("short u = 1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        assertEquals(0, exec("short u = 1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("short u = 1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("short u = 1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("short u = 1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("short u = 1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("short u = 1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("Short u = Short.valueOf((short)1); Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        assertEquals(0, exec("Short u = Short.valueOf((short)1); Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Short u = Short.valueOf((short)1); Character b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("Short u = Short.valueOf((short)1); Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("Short u = Short.valueOf((short)1); Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("Short u = Short.valueOf((short)1); Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Short u = Short.valueOf((short)1); Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("short u = 1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        assertEquals(0, exec("short u = 1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("short u = 1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("short u = 1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("short u = 1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("short u = 1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("short u = 1; def b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("Short u = Short.valueOf((short)1); def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        assertEquals(0, exec("Short u = Short.valueOf((short)1); def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Short u = Short.valueOf((short)1); def b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("Short u = Short.valueOf((short)1); def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("Short u = Short.valueOf((short)1); def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("Short u = Short.valueOf((short)1); def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Short u = Short.valueOf((short)1); def b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (short)1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (short)1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (short)1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (short)1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (short)1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (short)1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (short)1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (short)1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (short)1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (short)1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (short)1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (short)1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (short)1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (short)1; def b = Double.valueOf((double)1); b.compareTo(u);"));
    }

    public void testMethodCallCharacterToBoxedCasts() {
        expectScriptThrows(ClassCastException.class,
                () -> exec("char u = 1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("char u = 1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        assertEquals(0, exec("char u = 1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("char u = 1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("char u = 1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("char u = 1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("char u = 1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("Character u = Character.valueOf((char)1); Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Character u = Character.valueOf((char)1); Short b = Short.valueOf((short)1); b.compareTo(u);"));
        assertEquals(0, exec("Character u = Character.valueOf((char)1); Character b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("Character u = Character.valueOf((char)1); Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("Character u = Character.valueOf((char)1); Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("Character u = Character.valueOf((char)1); Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Character u = Character.valueOf((char)1); Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("char u = 1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("char u = 1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        assertEquals(0, exec("char u = 1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("char u = 1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("char u = 1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("char u = 1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("char u = 1; def b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("Character u = Character.valueOf((char)1); def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Character u = Character.valueOf((char)1); def b = Short.valueOf((short)1); b.compareTo(u);"));
        assertEquals(0, exec("Character u = Character.valueOf((char)1); def b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("Character u = Character.valueOf((char)1); def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("Character u = Character.valueOf((char)1); def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("Character u = Character.valueOf((char)1); def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Character u = Character.valueOf((char)1); def b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (char)1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (char)1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (char)1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (char)1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (char)1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (char)1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (char)1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (char)1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (char)1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (char)1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (char)1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (char)1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (char)1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (char)1; def b = Double.valueOf((double)1); b.compareTo(u);"));
    }

    public void testMethodCallIntegerToBoxedCasts() {
        expectScriptThrows(ClassCastException.class,
                () -> exec("int u = 1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("int u = 1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("int u = 1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("int u = 1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("int u = 1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("int u = 1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("int u = 1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("Integer u = Integer.valueOf((int)1); Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Integer u = Integer.valueOf((int)1); Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Integer u = Integer.valueOf((int)1); Character b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("Integer u = Integer.valueOf((int)1); Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("Integer u = Integer.valueOf((int)1); Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("Integer u = Integer.valueOf((int)1); Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Integer u = Integer.valueOf((int)1); Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("int u = 1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("int u = 1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("int u = 1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("int u = 1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("int u = 1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("int u = 1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("int u = 1; def b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("Integer u = Integer.valueOf((int)1); def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Integer u = Integer.valueOf((int)1); def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Integer u = Integer.valueOf((int)1); def b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("Integer u = Integer.valueOf((int)1); def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("Integer u = Integer.valueOf((int)1); def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("Integer u = Integer.valueOf((int)1); def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Integer u = Integer.valueOf((int)1); def b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (int)1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (int)1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (int)1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (int)1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (int)1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (int)1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (int)1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (int)1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (int)1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (int)1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (int)1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (int)1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (int)1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (int)1; def b = Double.valueOf((double)1); b.compareTo(u);"));
    }

    public void testMethodCallLongToBoxedCasts() {
        expectScriptThrows(ClassCastException.class,
                () -> exec("long u = 1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("long u = 1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("long u = 1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("long u = 1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("long u = 1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("long u = 1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("long u = 1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("Long u = Long.valueOf((long)1); Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Long u = Long.valueOf((long)1); Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Long u = Long.valueOf((long)1); Character b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Long u = Long.valueOf((long)1); Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("Long u = Long.valueOf((long)1); Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("Long u = Long.valueOf((long)1); Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Long u = Long.valueOf((long)1); Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("long u = 1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("long u = 1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("long u = 1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("long u = 1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("long u = 1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("long u = 1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("long u = 1; def b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("Long u = Long.valueOf((long)1); def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Long u = Long.valueOf((long)1); def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Long u = Long.valueOf((long)1); def b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Long u = Long.valueOf((long)1); def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("Long u = Long.valueOf((long)1); def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("Long u = Long.valueOf((long)1); def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Long u = Long.valueOf((long)1); def b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (long)1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (long)1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (long)1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (long)1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (long)1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (long)1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (long)1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (long)1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (long)1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (long)1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (long)1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (long)1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (long)1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (long)1; def b = Double.valueOf((double)1); b.compareTo(u);"));
    }

    public void testMethodCallFloatToBoxedCasts() {
        expectScriptThrows(ClassCastException.class,
                () -> exec("float u = 1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("float u = 1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("float u = 1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("float u = 1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("float u = 1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("float u = 1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("float u = 1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("Float u = Float.valueOf((float)1); Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Float u = Float.valueOf((float)1); Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Float u = Float.valueOf((float)1); Character b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Float u = Float.valueOf((float)1); Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Float u = Float.valueOf((float)1); Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("Float u = Float.valueOf((float)1); Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Float u = Float.valueOf((float)1); Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("float u = 1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("float u = 1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("float u = 1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("float u = 1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("float u = 1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("float u = 1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("float u = 1; def b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("Float u = Float.valueOf((float)1); def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Float u = Float.valueOf((float)1); def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Float u = Float.valueOf((float)1); def b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Float u = Float.valueOf((float)1); def b = Integer.valueOf((int)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Float u = Float.valueOf((float)1); def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("Float u = Float.valueOf((float)1); def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Float u = Float.valueOf((float)1); def b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (float)1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (float)1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (float)1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (float)1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (float)1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (float)1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (float)1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (float)1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (float)1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (float)1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (float)1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (float)1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (float)1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (float)1; def b = Double.valueOf((double)1); b.compareTo(u);"));
    }

    public void testMethodCallDoubleToBoxedCasts() {
        expectScriptThrows(ClassCastException.class,
                () -> exec("double u = 1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("double u = 1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("double u = 1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("double u = 1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("double u = 1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("double u = 1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("double u = 1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("Double u = Double.valueOf((double)1); Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Double u = Double.valueOf((double)1); Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Double u = Double.valueOf((double)1); Character b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Double u = Double.valueOf((double)1); Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Double u = Double.valueOf((double)1); Long b = Long.valueOf((long)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Double u = Double.valueOf((double)1); Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Double u = Double.valueOf((double)1); Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("double u = 1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("double u = 1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("double u = 1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("double u = 1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("double u = 1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("double u = 1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("double u = 1; def b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("Double u = Double.valueOf((double)1); def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Double u = Double.valueOf((double)1); def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Double u = Double.valueOf((double)1); def b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Double u = Double.valueOf((double)1); def b = Integer.valueOf((int)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Double u = Double.valueOf((double)1); def b = Long.valueOf((long)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("Double u = Double.valueOf((double)1); def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("Double u = Double.valueOf((double)1); def b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (double)1; Byte b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (double)1; Short b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (double)1; Character b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (double)1; Integer b = Integer.valueOf((int)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (double)1; Long b = Long.valueOf((long)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (double)1; Float b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (double)1; Double b = Double.valueOf((double)1); b.compareTo(u);"));

        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (double)1; def b = Byte.valueOf((byte)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (double)1; def b = Short.valueOf((short)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (double)1; def b = Character.valueOf((char)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (double)1; def b = Integer.valueOf((int)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (double)1; def b = Long.valueOf((long)1); b.compareTo(u);"));
        expectScriptThrows(ClassCastException.class,
                () -> exec("def u = (double)1; def b = Float.valueOf((float)1); b.compareTo(u);"));
        assertEquals(0, exec("def u = (double)1; def b = Double.valueOf((double)1); b.compareTo(u);"));
    }
}
