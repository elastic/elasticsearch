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

public class DefTests extends ScriptTestCase {
    public void testNot() {
        assertEquals(~1, exec("def x = (byte)1 return ~x"));
        assertEquals(~1, exec("def x = (short)1 return ~x"));
        assertEquals(~1, exec("def x = (char)1 return ~x"));
        assertEquals(~1, exec("def x = 1 return ~x"));
        assertEquals(~1L, exec("def x = 1L return ~x"));
    }

    public void testNeg() {
        assertEquals(-1, exec("def x = (byte)1 return -x"));
        assertEquals(-1, exec("def x = (short)1 return -x"));
        assertEquals(-1, exec("def x = (char)1 return -x"));
        assertEquals(-1, exec("def x = 1 return -x"));
        assertEquals(-1L, exec("def x = 1L return -x"));
        assertEquals(-1.0F, exec("def x = 1F return -x"));
        assertEquals(-1.0, exec("def x = 1.0 return -x"));
    }

    public void testMul() {
        assertEquals(4, exec("def x = (byte)2 def y = (byte)2 return x * y"));
        assertEquals(4, exec("def x = (short)2 def y = (byte)2 return x * y"));
        assertEquals(4, exec("def x = (char)2 def y = (byte)2 return x * y"));
        assertEquals(4, exec("def x = (int)2 def y = (byte)2 return x * y"));
        assertEquals(4L, exec("def x = (long)2 def y = (byte)2 return x * y"));
        assertEquals(4F, exec("def x = (float)2 def y = (byte)2 return x * y"));
        assertEquals(4D, exec("def x = (double)2 def y = (byte)2 return x * y"));

        assertEquals(4, exec("def x = (byte)2 def y = (short)2 return x * y"));
        assertEquals(4, exec("def x = (short)2 def y = (short)2 return x * y"));
        assertEquals(4, exec("def x = (char)2 def y = (short)2 return x * y"));
        assertEquals(4, exec("def x = (int)2 def y = (short)2 return x * y"));
        assertEquals(4L, exec("def x = (long)2 def y = (short)2 return x * y"));
        assertEquals(4F, exec("def x = (float)2 def y = (short)2 return x * y"));
        assertEquals(4D, exec("def x = (double)2 def y = (short)2 return x * y"));

        assertEquals(4, exec("def x = (byte)2 def y = (char)2 return x * y"));
        assertEquals(4, exec("def x = (short)2 def y = (char)2 return x * y"));
        assertEquals(4, exec("def x = (char)2 def y = (char)2 return x * y"));
        assertEquals(4, exec("def x = (int)2 def y = (char)2 return x * y"));
        assertEquals(4L, exec("def x = (long)2 def y = (char)2 return x * y"));
        assertEquals(4F, exec("def x = (float)2 def y = (char)2 return x * y"));
        assertEquals(4D, exec("def x = (double)2 def y = (char)2 return x * y"));

        assertEquals(4, exec("def x = (byte)2 def y = (int)2 return x * y"));
        assertEquals(4, exec("def x = (short)2 def y = (int)2 return x * y"));
        assertEquals(4, exec("def x = (char)2 def y = (int)2 return x * y"));
        assertEquals(4, exec("def x = (int)2 def y = (int)2 return x * y"));
        assertEquals(4L, exec("def x = (long)2 def y = (int)2 return x * y"));
        assertEquals(4F, exec("def x = (float)2 def y = (int)2 return x * y"));
        assertEquals(4D, exec("def x = (double)2 def y = (int)2 return x * y"));

        assertEquals(4L, exec("def x = (byte)2 def y = (long)2 return x * y"));
        assertEquals(4L, exec("def x = (short)2 def y = (long)2 return x * y"));
        assertEquals(4L, exec("def x = (char)2 def y = (long)2 return x * y"));
        assertEquals(4L, exec("def x = (int)2 def y = (long)2 return x * y"));
        assertEquals(4L, exec("def x = (long)2 def y = (long)2 return x * y"));
        assertEquals(4F, exec("def x = (float)2 def y = (long)2 return x * y"));
        assertEquals(4D, exec("def x = (double)2 def y = (long)2 return x * y"));

        assertEquals(4F, exec("def x = (byte)2 def y = (float)2 return x * y"));
        assertEquals(4F, exec("def x = (short)2 def y = (float)2 return x * y"));
        assertEquals(4F, exec("def x = (char)2 def y = (float)2 return x * y"));
        assertEquals(4F, exec("def x = (int)2 def y = (float)2 return x * y"));
        assertEquals(4F, exec("def x = (long)2 def y = (float)2 return x * y"));
        assertEquals(4F, exec("def x = (float)2 def y = (float)2 return x * y"));
        assertEquals(4D, exec("def x = (double)2 def y = (float)2 return x * y"));

        assertEquals(4D, exec("def x = (byte)2 def y = (double)2 return x * y"));
        assertEquals(4D, exec("def x = (short)2 def y = (double)2 return x * y"));
        assertEquals(4D, exec("def x = (char)2 def y = (double)2 return x * y"));
        assertEquals(4D, exec("def x = (int)2 def y = (double)2 return x * y"));
        assertEquals(4D, exec("def x = (long)2 def y = (double)2 return x * y"));
        assertEquals(4D, exec("def x = (float)2 def y = (double)2 return x * y"));
        assertEquals(4D, exec("def x = (double)2 def y = (double)2 return x * y"));

        assertEquals(4, exec("def x = (Byte)2 def y = (byte)2 return x * y"));
        assertEquals(4, exec("def x = (Short)2 def y = (short)2 return x * y"));
        assertEquals(4, exec("def x = (Character)2 def y = (char)2 return x * y"));
        assertEquals(4, exec("def x = (Integer)2 def y = (int)2 return x * y"));
        assertEquals(4L, exec("def x = (Long)2 def y = (long)2 return x * y"));
        assertEquals(4F, exec("def x = (Float)2 def y = (float)2 return x * y"));
        assertEquals(4D, exec("def x = (Double)2 def y = (double)2 return x * y"));
    }

    public void testDiv() {
        assertEquals(1, exec("def x = (byte)2 def y = (byte)2 return x / y"));
        assertEquals(1, exec("def x = (short)2 def y = (byte)2 return x / y"));
        assertEquals(1, exec("def x = (char)2 def y = (byte)2 return x / y"));
        assertEquals(1, exec("def x = (int)2 def y = (byte)2 return x / y"));
        assertEquals(1L, exec("def x = (long)2 def y = (byte)2 return x / y"));
        assertEquals(1F, exec("def x = (float)2 def y = (byte)2 return x / y"));
        assertEquals(1D, exec("def x = (double)2 def y = (byte)2 return x / y"));

        assertEquals(1, exec("def x = (byte)2 def y = (short)2 return x / y"));
        assertEquals(1, exec("def x = (short)2 def y = (short)2 return x / y"));
        assertEquals(1, exec("def x = (char)2 def y = (short)2 return x / y"));
        assertEquals(1, exec("def x = (int)2 def y = (short)2 return x / y"));
        assertEquals(1L, exec("def x = (long)2 def y = (short)2 return x / y"));
        assertEquals(1F, exec("def x = (float)2 def y = (short)2 return x / y"));
        assertEquals(1D, exec("def x = (double)2 def y = (short)2 return x / y"));

        assertEquals(1, exec("def x = (byte)2 def y = (char)2 return x / y"));
        assertEquals(1, exec("def x = (short)2 def y = (char)2 return x / y"));
        assertEquals(1, exec("def x = (char)2 def y = (char)2 return x / y"));
        assertEquals(1, exec("def x = (int)2 def y = (char)2 return x / y"));
        assertEquals(1L, exec("def x = (long)2 def y = (char)2 return x / y"));
        assertEquals(1F, exec("def x = (float)2 def y = (char)2 return x / y"));
        assertEquals(1D, exec("def x = (double)2 def y = (char)2 return x / y"));

        assertEquals(1, exec("def x = (byte)2 def y = (int)2 return x / y"));
        assertEquals(1, exec("def x = (short)2 def y = (int)2 return x / y"));
        assertEquals(1, exec("def x = (char)2 def y = (int)2 return x / y"));
        assertEquals(1, exec("def x = (int)2 def y = (int)2 return x / y"));
        assertEquals(1L, exec("def x = (long)2 def y = (int)2 return x / y"));
        assertEquals(1F, exec("def x = (float)2 def y = (int)2 return x / y"));
        assertEquals(1D, exec("def x = (double)2 def y = (int)2 return x / y"));

        assertEquals(1L, exec("def x = (byte)2 def y = (long)2 return x / y"));
        assertEquals(1L, exec("def x = (short)2 def y = (long)2 return x / y"));
        assertEquals(1L, exec("def x = (char)2 def y = (long)2 return x / y"));
        assertEquals(1L, exec("def x = (int)2 def y = (long)2 return x / y"));
        assertEquals(1L, exec("def x = (long)2 def y = (long)2 return x / y"));
        assertEquals(1F, exec("def x = (float)2 def y = (long)2 return x / y"));
        assertEquals(1D, exec("def x = (double)2 def y = (long)2 return x / y"));

        assertEquals(1F, exec("def x = (byte)2 def y = (float)2 return x / y"));
        assertEquals(1F, exec("def x = (short)2 def y = (float)2 return x / y"));
        assertEquals(1F, exec("def x = (char)2 def y = (float)2 return x / y"));
        assertEquals(1F, exec("def x = (int)2 def y = (float)2 return x / y"));
        assertEquals(1F, exec("def x = (long)2 def y = (float)2 return x / y"));
        assertEquals(1F, exec("def x = (float)2 def y = (float)2 return x / y"));
        assertEquals(1D, exec("def x = (double)2 def y = (float)2 return x / y"));

        assertEquals(1D, exec("def x = (byte)2 def y = (double)2 return x / y"));
        assertEquals(1D, exec("def x = (short)2 def y = (double)2 return x / y"));
        assertEquals(1D, exec("def x = (char)2 def y = (double)2 return x / y"));
        assertEquals(1D, exec("def x = (int)2 def y = (double)2 return x / y"));
        assertEquals(1D, exec("def x = (long)2 def y = (double)2 return x / y"));
        assertEquals(1D, exec("def x = (float)2 def y = (double)2 return x / y"));
        assertEquals(1D, exec("def x = (double)2 def y = (double)2 return x / y"));

        assertEquals(1, exec("def x = (Byte)2 def y = (byte)2 return x / y"));
        assertEquals(1, exec("def x = (Short)2 def y = (short)2 return x / y"));
        assertEquals(1, exec("def x = (Character)2 def y = (char)2 return x / y"));
        assertEquals(1, exec("def x = (Integer)2 def y = (int)2 return x / y"));
        assertEquals(1L, exec("def x = (Long)2 def y = (long)2 return x / y"));
        assertEquals(1F, exec("def x = (Float)2 def y = (float)2 return x / y"));
        assertEquals(1D, exec("def x = (Double)2 def y = (double)2 return x / y"));
    }

    public void testRem() {
        assertEquals(0, exec("def x = (byte)2 def y = (byte)2 return x % y"));
        assertEquals(0, exec("def x = (short)2 def y = (byte)2 return x % y"));
        assertEquals(0, exec("def x = (char)2 def y = (byte)2 return x % y"));
        assertEquals(0, exec("def x = (int)2 def y = (byte)2 return x % y"));
        assertEquals(0L, exec("def x = (long)2 def y = (byte)2 return x % y"));
        assertEquals(0F, exec("def x = (float)2 def y = (byte)2 return x % y"));
        assertEquals(0D, exec("def x = (double)2 def y = (byte)2 return x % y"));

        assertEquals(0, exec("def x = (byte)2 def y = (short)2 return x % y"));
        assertEquals(0, exec("def x = (short)2 def y = (short)2 return x % y"));
        assertEquals(0, exec("def x = (char)2 def y = (short)2 return x % y"));
        assertEquals(0, exec("def x = (int)2 def y = (short)2 return x % y"));
        assertEquals(0L, exec("def x = (long)2 def y = (short)2 return x % y"));
        assertEquals(0F, exec("def x = (float)2 def y = (short)2 return x % y"));
        assertEquals(0D, exec("def x = (double)2 def y = (short)2 return x % y"));

        assertEquals(0, exec("def x = (byte)2 def y = (char)2 return x % y"));
        assertEquals(0, exec("def x = (short)2 def y = (char)2 return x % y"));
        assertEquals(0, exec("def x = (char)2 def y = (char)2 return x % y"));
        assertEquals(0, exec("def x = (int)2 def y = (char)2 return x % y"));
        assertEquals(0L, exec("def x = (long)2 def y = (char)2 return x % y"));
        assertEquals(0F, exec("def x = (float)2 def y = (char)2 return x % y"));
        assertEquals(0D, exec("def x = (double)2 def y = (char)2 return x % y"));

        assertEquals(0, exec("def x = (byte)2 def y = (int)2 return x % y"));
        assertEquals(0, exec("def x = (short)2 def y = (int)2 return x % y"));
        assertEquals(0, exec("def x = (char)2 def y = (int)2 return x % y"));
        assertEquals(0, exec("def x = (int)2 def y = (int)2 return x % y"));
        assertEquals(0L, exec("def x = (long)2 def y = (int)2 return x % y"));
        assertEquals(0F, exec("def x = (float)2 def y = (int)2 return x % y"));
        assertEquals(0D, exec("def x = (double)2 def y = (int)2 return x % y"));

        assertEquals(0L, exec("def x = (byte)2 def y = (long)2 return x % y"));
        assertEquals(0L, exec("def x = (short)2 def y = (long)2 return x % y"));
        assertEquals(0L, exec("def x = (char)2 def y = (long)2 return x % y"));
        assertEquals(0L, exec("def x = (int)2 def y = (long)2 return x % y"));
        assertEquals(0L, exec("def x = (long)2 def y = (long)2 return x % y"));
        assertEquals(0F, exec("def x = (float)2 def y = (long)2 return x % y"));
        assertEquals(0D, exec("def x = (double)2 def y = (long)2 return x % y"));

        assertEquals(0F, exec("def x = (byte)2 def y = (float)2 return x % y"));
        assertEquals(0F, exec("def x = (short)2 def y = (float)2 return x % y"));
        assertEquals(0F, exec("def x = (char)2 def y = (float)2 return x % y"));
        assertEquals(0F, exec("def x = (int)2 def y = (float)2 return x % y"));
        assertEquals(0F, exec("def x = (long)2 def y = (float)2 return x % y"));
        assertEquals(0F, exec("def x = (float)2 def y = (float)2 return x % y"));
        assertEquals(0D, exec("def x = (double)2 def y = (float)2 return x % y"));

        assertEquals(0D, exec("def x = (byte)2 def y = (double)2 return x % y"));
        assertEquals(0D, exec("def x = (short)2 def y = (double)2 return x % y"));
        assertEquals(0D, exec("def x = (char)2 def y = (double)2 return x % y"));
        assertEquals(0D, exec("def x = (int)2 def y = (double)2 return x % y"));
        assertEquals(0D, exec("def x = (long)2 def y = (double)2 return x % y"));
        assertEquals(0D, exec("def x = (float)2 def y = (double)2 return x % y"));
        assertEquals(0D, exec("def x = (double)2 def y = (double)2 return x % y"));

        assertEquals(0, exec("def x = (Byte)2 def y = (byte)2 return x % y"));
        assertEquals(0, exec("def x = (Short)2 def y = (short)2 return x % y"));
        assertEquals(0, exec("def x = (Character)2 def y = (char)2 return x % y"));
        assertEquals(0, exec("def x = (Integer)2 def y = (int)2 return x % y"));
        assertEquals(0L, exec("def x = (Long)2 def y = (long)2 return x % y"));
        assertEquals(0F, exec("def x = (Float)2 def y = (float)2 return x % y"));
        assertEquals(0D, exec("def x = (Double)2 def y = (double)2 return x % y"));
    }
    
    public void testAdd() {
        assertEquals(2, exec("def x = (byte)1 def y = (byte)1 return x + y"));
        assertEquals(2, exec("def x = (short)1 def y = (byte)1 return x + y"));
        assertEquals(2, exec("def x = (char)1 def y = (byte)1 return x + y"));
        assertEquals(2, exec("def x = (int)1 def y = (byte)1 return x + y"));
        assertEquals(2L, exec("def x = (long)1 def y = (byte)1 return x + y"));
        assertEquals(2F, exec("def x = (float)1 def y = (byte)1 return x + y"));
        assertEquals(2D, exec("def x = (double)1 def y = (byte)1 return x + y"));

        assertEquals(2, exec("def x = (byte)1 def y = (short)1 return x + y"));
        assertEquals(2, exec("def x = (short)1 def y = (short)1 return x + y"));
        assertEquals(2, exec("def x = (char)1 def y = (short)1 return x + y"));
        assertEquals(2, exec("def x = (int)1 def y = (short)1 return x + y"));
        assertEquals(2L, exec("def x = (long)1 def y = (short)1 return x + y"));
        assertEquals(2F, exec("def x = (float)1 def y = (short)1 return x + y"));
        assertEquals(2D, exec("def x = (double)1 def y = (short)1 return x + y"));

        assertEquals(2, exec("def x = (byte)1 def y = (char)1 return x + y"));
        assertEquals(2, exec("def x = (short)1 def y = (char)1 return x + y"));
        assertEquals(2, exec("def x = (char)1 def y = (char)1 return x + y"));
        assertEquals(2, exec("def x = (int)1 def y = (char)1 return x + y"));
        assertEquals(2L, exec("def x = (long)1 def y = (char)1 return x + y"));
        assertEquals(2F, exec("def x = (float)1 def y = (char)1 return x + y"));
        assertEquals(2D, exec("def x = (double)1 def y = (char)1 return x + y"));

        assertEquals(2, exec("def x = (byte)1 def y = (int)1 return x + y"));
        assertEquals(2, exec("def x = (short)1 def y = (int)1 return x + y"));
        assertEquals(2, exec("def x = (char)1 def y = (int)1 return x + y"));
        assertEquals(2, exec("def x = (int)1 def y = (int)1 return x + y"));
        assertEquals(2L, exec("def x = (long)1 def y = (int)1 return x + y"));
        assertEquals(2F, exec("def x = (float)1 def y = (int)1 return x + y"));
        assertEquals(2D, exec("def x = (double)1 def y = (int)1 return x + y"));

        assertEquals(2L, exec("def x = (byte)1 def y = (long)1 return x + y"));
        assertEquals(2L, exec("def x = (short)1 def y = (long)1 return x + y"));
        assertEquals(2L, exec("def x = (char)1 def y = (long)1 return x + y"));
        assertEquals(2L, exec("def x = (int)1 def y = (long)1 return x + y"));
        assertEquals(2L, exec("def x = (long)1 def y = (long)1 return x + y"));
        assertEquals(2F, exec("def x = (float)1 def y = (long)1 return x + y"));
        assertEquals(2D, exec("def x = (double)1 def y = (long)1 return x + y"));

        assertEquals(2F, exec("def x = (byte)1 def y = (float)1 return x + y"));
        assertEquals(2F, exec("def x = (short)1 def y = (float)1 return x + y"));
        assertEquals(2F, exec("def x = (char)1 def y = (float)1 return x + y"));
        assertEquals(2F, exec("def x = (int)1 def y = (float)1 return x + y"));
        assertEquals(2F, exec("def x = (long)1 def y = (float)1 return x + y"));
        assertEquals(2F, exec("def x = (float)1 def y = (float)1 return x + y"));
        assertEquals(2D, exec("def x = (double)1 def y = (float)1 return x + y"));

        assertEquals(2D, exec("def x = (byte)1 def y = (double)1 return x + y"));
        assertEquals(2D, exec("def x = (short)1 def y = (double)1 return x + y"));
        assertEquals(2D, exec("def x = (char)1 def y = (double)1 return x + y"));
        assertEquals(2D, exec("def x = (int)1 def y = (double)1 return x + y"));
        assertEquals(2D, exec("def x = (long)1 def y = (double)1 return x + y"));
        assertEquals(2D, exec("def x = (float)1 def y = (double)1 return x + y"));
        assertEquals(2D, exec("def x = (double)1 def y = (double)1 return x + y"));

        assertEquals(2, exec("def x = (Byte)1 def y = (byte)1 return x + y"));
        assertEquals(2, exec("def x = (Short)1 def y = (short)1 return x + y"));
        assertEquals(2, exec("def x = (Character)1 def y = (char)1 return x + y"));
        assertEquals(2, exec("def x = (Integer)1 def y = (int)1 return x + y"));
        assertEquals(2L, exec("def x = (Long)1 def y = (long)1 return x + y"));
        assertEquals(2F, exec("def x = (Float)1 def y = (float)1 return x + y"));
        assertEquals(2D, exec("def x = (Double)1 def y = (double)1 return x + y"));
    }

    public void testSub() {
        assertEquals(0, exec("def x = (byte)1 def y = (byte)1 return x - y"));
        assertEquals(0, exec("def x = (short)1 def y = (byte)1 return x - y"));
        assertEquals(0, exec("def x = (char)1 def y = (byte)1 return x - y"));
        assertEquals(0, exec("def x = (int)1 def y = (byte)1 return x - y"));
        assertEquals(0L, exec("def x = (long)1 def y = (byte)1 return x - y"));
        assertEquals(0F, exec("def x = (float)1 def y = (byte)1 return x - y"));
        assertEquals(0D, exec("def x = (double)1 def y = (byte)1 return x - y"));

        assertEquals(0, exec("def x = (byte)1 def y = (short)1 return x - y"));
        assertEquals(0, exec("def x = (short)1 def y = (short)1 return x - y"));
        assertEquals(0, exec("def x = (char)1 def y = (short)1 return x - y"));
        assertEquals(0, exec("def x = (int)1 def y = (short)1 return x - y"));
        assertEquals(0L, exec("def x = (long)1 def y = (short)1 return x - y"));
        assertEquals(0F, exec("def x = (float)1 def y = (short)1 return x - y"));
        assertEquals(0D, exec("def x = (double)1 def y = (short)1 return x - y"));

        assertEquals(0, exec("def x = (byte)1 def y = (char)1 return x - y"));
        assertEquals(0, exec("def x = (short)1 def y = (char)1 return x - y"));
        assertEquals(0, exec("def x = (char)1 def y = (char)1 return x - y"));
        assertEquals(0, exec("def x = (int)1 def y = (char)1 return x - y"));
        assertEquals(0L, exec("def x = (long)1 def y = (char)1 return x - y"));
        assertEquals(0F, exec("def x = (float)1 def y = (char)1 return x - y"));
        assertEquals(0D, exec("def x = (double)1 def y = (char)1 return x - y"));

        assertEquals(0, exec("def x = (byte)1 def y = (int)1 return x - y"));
        assertEquals(0, exec("def x = (short)1 def y = (int)1 return x - y"));
        assertEquals(0, exec("def x = (char)1 def y = (int)1 return x - y"));
        assertEquals(0, exec("def x = (int)1 def y = (int)1 return x - y"));
        assertEquals(0L, exec("def x = (long)1 def y = (int)1 return x - y"));
        assertEquals(0F, exec("def x = (float)1 def y = (int)1 return x - y"));
        assertEquals(0D, exec("def x = (double)1 def y = (int)1 return x - y"));

        assertEquals(0L, exec("def x = (byte)1 def y = (long)1 return x - y"));
        assertEquals(0L, exec("def x = (short)1 def y = (long)1 return x - y"));
        assertEquals(0L, exec("def x = (char)1 def y = (long)1 return x - y"));
        assertEquals(0L, exec("def x = (int)1 def y = (long)1 return x - y"));
        assertEquals(0L, exec("def x = (long)1 def y = (long)1 return x - y"));
        assertEquals(0F, exec("def x = (float)1 def y = (long)1 return x - y"));
        assertEquals(0D, exec("def x = (double)1 def y = (long)1 return x - y"));

        assertEquals(0F, exec("def x = (byte)1 def y = (float)1 return x - y"));
        assertEquals(0F, exec("def x = (short)1 def y = (float)1 return x - y"));
        assertEquals(0F, exec("def x = (char)1 def y = (float)1 return x - y"));
        assertEquals(0F, exec("def x = (int)1 def y = (float)1 return x - y"));
        assertEquals(0F, exec("def x = (long)1 def y = (float)1 return x - y"));
        assertEquals(0F, exec("def x = (float)1 def y = (float)1 return x - y"));
        assertEquals(0D, exec("def x = (double)1 def y = (float)1 return x - y"));

        assertEquals(0D, exec("def x = (byte)1 def y = (double)1 return x - y"));
        assertEquals(0D, exec("def x = (short)1 def y = (double)1 return x - y"));
        assertEquals(0D, exec("def x = (char)1 def y = (double)1 return x - y"));
        assertEquals(0D, exec("def x = (int)1 def y = (double)1 return x - y"));
        assertEquals(0D, exec("def x = (long)1 def y = (double)1 return x - y"));
        assertEquals(0D, exec("def x = (float)1 def y = (double)1 return x - y"));
        assertEquals(0D, exec("def x = (double)1 def y = (double)1 return x - y"));

        assertEquals(0, exec("def x = (Byte)1 def y = (byte)1 return x - y"));
        assertEquals(0, exec("def x = (Short)1 def y = (short)1 return x - y"));
        assertEquals(0, exec("def x = (Character)1 def y = (char)1 return x - y"));
        assertEquals(0, exec("def x = (Integer)1 def y = (int)1 return x - y"));
        assertEquals(0L, exec("def x = (Long)1 def y = (long)1 return x - y"));
        assertEquals(0F, exec("def x = (Float)1 def y = (float)1 return x - y"));
        assertEquals(0D, exec("def x = (Double)1 def y = (double)1 return x - y"));
    }

    public void testLsh() {
        assertEquals(2, exec("def x = (byte)1 def y = (byte)1 return x << y"));
        assertEquals(2, exec("def x = (short)1 def y = (byte)1 return x << y"));
        assertEquals(2, exec("def x = (char)1 def y = (byte)1 return x << y"));
        assertEquals(2, exec("def x = (int)1 def y = (byte)1 return x << y"));
        assertEquals(2L, exec("def x = (long)1 def y = (byte)1 return x << y"));
        assertEquals(2L, exec("def x = (float)1 def y = (byte)1 return x << y"));
        assertEquals(2L, exec("def x = (double)1 def y = (byte)1 return x << y"));

        assertEquals(2, exec("def x = (byte)1 def y = (short)1 return x << y"));
        assertEquals(2, exec("def x = (short)1 def y = (short)1 return x << y"));
        assertEquals(2, exec("def x = (char)1 def y = (short)1 return x << y"));
        assertEquals(2, exec("def x = (int)1 def y = (short)1 return x << y"));
        assertEquals(2L, exec("def x = (long)1 def y = (short)1 return x << y"));
        assertEquals(2L, exec("def x = (float)1 def y = (short)1 return x << y"));
        assertEquals(2L, exec("def x = (double)1 def y = (short)1 return x << y"));

        assertEquals(2, exec("def x = (byte)1 def y = (char)1 return x << y"));
        assertEquals(2, exec("def x = (short)1 def y = (char)1 return x << y"));
        assertEquals(2, exec("def x = (char)1 def y = (char)1 return x << y"));
        assertEquals(2, exec("def x = (int)1 def y = (char)1 return x << y"));
        assertEquals(2L, exec("def x = (long)1 def y = (char)1 return x << y"));
        assertEquals(2L, exec("def x = (float)1 def y = (char)1 return x << y"));
        assertEquals(2L, exec("def x = (double)1 def y = (char)1 return x << y"));

        assertEquals(2, exec("def x = (byte)1 def y = (int)1 return x << y"));
        assertEquals(2, exec("def x = (short)1 def y = (int)1 return x << y"));
        assertEquals(2, exec("def x = (char)1 def y = (int)1 return x << y"));
        assertEquals(2, exec("def x = (int)1 def y = (int)1 return x << y"));
        assertEquals(2L, exec("def x = (long)1 def y = (int)1 return x << y"));
        assertEquals(2L, exec("def x = (float)1 def y = (int)1 return x << y"));
        assertEquals(2L, exec("def x = (double)1 def y = (int)1 return x << y"));

        assertEquals(2L, exec("def x = (byte)1 def y = (long)1 return x << y"));
        assertEquals(2L, exec("def x = (short)1 def y = (long)1 return x << y"));
        assertEquals(2L, exec("def x = (char)1 def y = (long)1 return x << y"));
        assertEquals(2L, exec("def x = (int)1 def y = (long)1 return x << y"));
        assertEquals(2L, exec("def x = (long)1 def y = (long)1 return x << y"));
        assertEquals(2L, exec("def x = (float)1 def y = (long)1 return x << y"));
        assertEquals(2L, exec("def x = (double)1 def y = (long)1 return x << y"));

        assertEquals(2L, exec("def x = (byte)1 def y = (float)1 return x << y"));
        assertEquals(2L, exec("def x = (short)1 def y = (float)1 return x << y"));
        assertEquals(2L, exec("def x = (char)1 def y = (float)1 return x << y"));
        assertEquals(2L, exec("def x = (int)1 def y = (float)1 return x << y"));
        assertEquals(2L, exec("def x = (long)1 def y = (float)1 return x << y"));
        assertEquals(2L, exec("def x = (float)1 def y = (float)1 return x << y"));
        assertEquals(2L, exec("def x = (double)1 def y = (float)1 return x << y"));

        assertEquals(2L, exec("def x = (byte)1 def y = (double)1 return x << y"));
        assertEquals(2L, exec("def x = (short)1 def y = (double)1 return x << y"));
        assertEquals(2L, exec("def x = (char)1 def y = (double)1 return x << y"));
        assertEquals(2L, exec("def x = (int)1 def y = (double)1 return x << y"));
        assertEquals(2L, exec("def x = (long)1 def y = (double)1 return x << y"));
        assertEquals(2L, exec("def x = (float)1 def y = (double)1 return x << y"));
        assertEquals(2L, exec("def x = (double)1 def y = (double)1 return x << y"));

        assertEquals(2, exec("def x = (Byte)1 def y = (byte)1 return x << y"));
        assertEquals(2, exec("def x = (Short)1 def y = (short)1 return x << y"));
        assertEquals(2, exec("def x = (Character)1 def y = (char)1 return x << y"));
        assertEquals(2, exec("def x = (Integer)1 def y = (int)1 return x << y"));
        assertEquals(2L, exec("def x = (Long)1 def y = (long)1 return x << y"));
        assertEquals(2L, exec("def x = (Float)1 def y = (float)1 return x << y"));
        assertEquals(2L, exec("def x = (Double)1 def y = (double)1 return x << y"));
    }

    public void testRsh() {
        assertEquals(2, exec("def x = (byte)4 def y = (byte)1 return x >> y"));
        assertEquals(2, exec("def x = (short)4 def y = (byte)1 return x >> y"));
        assertEquals(2, exec("def x = (char)4 def y = (byte)1 return x >> y"));
        assertEquals(2, exec("def x = (int)4 def y = (byte)1 return x >> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (byte)1 return x >> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (byte)1 return x >> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (byte)1 return x >> y"));

        assertEquals(2, exec("def x = (byte)4 def y = (short)1 return x >> y"));
        assertEquals(2, exec("def x = (short)4 def y = (short)1 return x >> y"));
        assertEquals(2, exec("def x = (char)4 def y = (short)1 return x >> y"));
        assertEquals(2, exec("def x = (int)4 def y = (short)1 return x >> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (short)1 return x >> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (short)1 return x >> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (short)1 return x >> y"));

        assertEquals(2, exec("def x = (byte)4 def y = (char)1 return x >> y"));
        assertEquals(2, exec("def x = (short)4 def y = (char)1 return x >> y"));
        assertEquals(2, exec("def x = (char)4 def y = (char)1 return x >> y"));
        assertEquals(2, exec("def x = (int)4 def y = (char)1 return x >> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (char)1 return x >> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (char)1 return x >> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (char)1 return x >> y"));

        assertEquals(2, exec("def x = (byte)4 def y = (int)1 return x >> y"));
        assertEquals(2, exec("def x = (short)4 def y = (int)1 return x >> y"));
        assertEquals(2, exec("def x = (char)4 def y = (int)1 return x >> y"));
        assertEquals(2, exec("def x = (int)4 def y = (int)1 return x >> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (int)1 return x >> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (int)1 return x >> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (int)1 return x >> y"));

        assertEquals(2L, exec("def x = (byte)4 def y = (long)1 return x >> y"));
        assertEquals(2L, exec("def x = (short)4 def y = (long)1 return x >> y"));
        assertEquals(2L, exec("def x = (char)4 def y = (long)1 return x >> y"));
        assertEquals(2L, exec("def x = (int)4 def y = (long)1 return x >> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (long)1 return x >> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (long)1 return x >> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (long)1 return x >> y"));

        assertEquals(2L, exec("def x = (byte)4 def y = (float)1 return x >> y"));
        assertEquals(2L, exec("def x = (short)4 def y = (float)1 return x >> y"));
        assertEquals(2L, exec("def x = (char)4 def y = (float)1 return x >> y"));
        assertEquals(2L, exec("def x = (int)4 def y = (float)1 return x >> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (float)1 return x >> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (float)1 return x >> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (float)1 return x >> y"));

        assertEquals(2L, exec("def x = (byte)4 def y = (double)1 return x >> y"));
        assertEquals(2L, exec("def x = (short)4 def y = (double)1 return x >> y"));
        assertEquals(2L, exec("def x = (char)4 def y = (double)1 return x >> y"));
        assertEquals(2L, exec("def x = (int)4 def y = (double)1 return x >> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (double)1 return x >> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (double)1 return x >> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (double)1 return x >> y"));

        assertEquals(2, exec("def x = (Byte)4 def y = (byte)1 return x >> y"));
        assertEquals(2, exec("def x = (Short)4 def y = (short)1 return x >> y"));
        assertEquals(2, exec("def x = (Character)4 def y = (char)1 return x >> y"));
        assertEquals(2, exec("def x = (Integer)4 def y = (int)1 return x >> y"));
        assertEquals(2L, exec("def x = (Long)4 def y = (long)1 return x >> y"));
        assertEquals(2L, exec("def x = (Float)4 def y = (float)1 return x >> y"));
        assertEquals(2L, exec("def x = (Double)4 def y = (double)1 return x >> y"));
    }

    public void testUsh() {
        assertEquals(2, exec("def x = (byte)4 def y = (byte)1 return x >>> y"));
        assertEquals(2, exec("def x = (short)4 def y = (byte)1 return x >>> y"));
        assertEquals(2, exec("def x = (char)4 def y = (byte)1 return x >>> y"));
        assertEquals(2, exec("def x = (int)4 def y = (byte)1 return x >>> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (byte)1 return x >>> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (byte)1 return x >>> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (byte)1 return x >>> y"));

        assertEquals(2, exec("def x = (byte)4 def y = (short)1 return x >>> y"));
        assertEquals(2, exec("def x = (short)4 def y = (short)1 return x >>> y"));
        assertEquals(2, exec("def x = (char)4 def y = (short)1 return x >>> y"));
        assertEquals(2, exec("def x = (int)4 def y = (short)1 return x >>> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (short)1 return x >>> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (short)1 return x >>> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (short)1 return x >>> y"));

        assertEquals(2, exec("def x = (byte)4 def y = (char)1 return x >>> y"));
        assertEquals(2, exec("def x = (short)4 def y = (char)1 return x >>> y"));
        assertEquals(2, exec("def x = (char)4 def y = (char)1 return x >>> y"));
        assertEquals(2, exec("def x = (int)4 def y = (char)1 return x >>> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (char)1 return x >>> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (char)1 return x >>> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (char)1 return x >>> y"));

        assertEquals(2, exec("def x = (byte)4 def y = (int)1 return x >>> y"));
        assertEquals(2, exec("def x = (short)4 def y = (int)1 return x >>> y"));
        assertEquals(2, exec("def x = (char)4 def y = (int)1 return x >>> y"));
        assertEquals(2, exec("def x = (int)4 def y = (int)1 return x >>> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (int)1 return x >>> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (int)1 return x >>> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (int)1 return x >>> y"));

        assertEquals(2L, exec("def x = (byte)4 def y = (long)1 return x >>> y"));
        assertEquals(2L, exec("def x = (short)4 def y = (long)1 return x >>> y"));
        assertEquals(2L, exec("def x = (char)4 def y = (long)1 return x >>> y"));
        assertEquals(2L, exec("def x = (int)4 def y = (long)1 return x >>> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (long)1 return x >>> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (long)1 return x >>> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (long)1 return x >>> y"));

        assertEquals(2L, exec("def x = (byte)4 def y = (float)1 return x >>> y"));
        assertEquals(2L, exec("def x = (short)4 def y = (float)1 return x >>> y"));
        assertEquals(2L, exec("def x = (char)4 def y = (float)1 return x >>> y"));
        assertEquals(2L, exec("def x = (int)4 def y = (float)1 return x >>> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (float)1 return x >>> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (float)1 return x >>> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (float)1 return x >>> y"));

        assertEquals(2L, exec("def x = (byte)4 def y = (double)1 return x >>> y"));
        assertEquals(2L, exec("def x = (short)4 def y = (double)1 return x >>> y"));
        assertEquals(2L, exec("def x = (char)4 def y = (double)1 return x >>> y"));
        assertEquals(2L, exec("def x = (int)4 def y = (double)1 return x >>> y"));
        assertEquals(2L, exec("def x = (long)4 def y = (double)1 return x >>> y"));
        assertEquals(2L, exec("def x = (float)4 def y = (double)1 return x >>> y"));
        assertEquals(2L, exec("def x = (double)4 def y = (double)1 return x >>> y"));

        assertEquals(2, exec("def x = (Byte)4 def y = (byte)1 return x >>> y"));
        assertEquals(2, exec("def x = (Short)4 def y = (short)1 return x >>> y"));
        assertEquals(2, exec("def x = (Character)4 def y = (char)1 return x >>> y"));
        assertEquals(2, exec("def x = (Integer)4 def y = (int)1 return x >>> y"));
        assertEquals(2L, exec("def x = (Long)4 def y = (long)1 return x >>> y"));
        assertEquals(2L, exec("def x = (Float)4 def y = (float)1 return x >>> y"));
        assertEquals(2L, exec("def x = (Double)4 def y = (double)1 return x >>> y"));
    }

    public void testAnd() {
        assertEquals(0, exec("def x = (byte)4 def y = (byte)1 return x & y"));
        assertEquals(0, exec("def x = (short)4 def y = (byte)1 return x & y"));
        assertEquals(0, exec("def x = (char)4 def y = (byte)1 return x & y"));
        assertEquals(0, exec("def x = (int)4 def y = (byte)1 return x & y"));
        assertEquals(0L, exec("def x = (long)4 def y = (byte)1 return x & y"));
        assertEquals(0L, exec("def x = (float)4 def y = (byte)1 return x & y"));
        assertEquals(0L, exec("def x = (double)4 def y = (byte)1 return x & y"));

        assertEquals(0, exec("def x = (byte)4 def y = (short)1 return x & y"));
        assertEquals(0, exec("def x = (short)4 def y = (short)1 return x & y"));
        assertEquals(0, exec("def x = (char)4 def y = (short)1 return x & y"));
        assertEquals(0, exec("def x = (int)4 def y = (short)1 return x & y"));
        assertEquals(0L, exec("def x = (long)4 def y = (short)1 return x & y"));
        assertEquals(0L, exec("def x = (float)4 def y = (short)1 return x & y"));
        assertEquals(0L, exec("def x = (double)4 def y = (short)1 return x & y"));

        assertEquals(0, exec("def x = (byte)4 def y = (char)1 return x & y"));
        assertEquals(0, exec("def x = (short)4 def y = (char)1 return x & y"));
        assertEquals(0, exec("def x = (char)4 def y = (char)1 return x & y"));
        assertEquals(0, exec("def x = (int)4 def y = (char)1 return x & y"));
        assertEquals(0L, exec("def x = (long)4 def y = (char)1 return x & y"));
        assertEquals(0L, exec("def x = (float)4 def y = (char)1 return x & y"));
        assertEquals(0L, exec("def x = (double)4 def y = (char)1 return x & y"));

        assertEquals(0, exec("def x = (byte)4 def y = (int)1 return x & y"));
        assertEquals(0, exec("def x = (short)4 def y = (int)1 return x & y"));
        assertEquals(0, exec("def x = (char)4 def y = (int)1 return x & y"));
        assertEquals(0, exec("def x = (int)4 def y = (int)1 return x & y"));
        assertEquals(0L, exec("def x = (long)4 def y = (int)1 return x & y"));
        assertEquals(0L, exec("def x = (float)4 def y = (int)1 return x & y"));
        assertEquals(0L, exec("def x = (double)4 def y = (int)1 return x & y"));

        assertEquals(0L, exec("def x = (byte)4 def y = (long)1 return x & y"));
        assertEquals(0L, exec("def x = (short)4 def y = (long)1 return x & y"));
        assertEquals(0L, exec("def x = (char)4 def y = (long)1 return x & y"));
        assertEquals(0L, exec("def x = (int)4 def y = (long)1 return x & y"));
        assertEquals(0L, exec("def x = (long)4 def y = (long)1 return x & y"));
        assertEquals(0L, exec("def x = (float)4 def y = (long)1 return x & y"));
        assertEquals(0L, exec("def x = (double)4 def y = (long)1 return x & y"));

        assertEquals(0L, exec("def x = (byte)4 def y = (float)1 return x & y"));
        assertEquals(0L, exec("def x = (short)4 def y = (float)1 return x & y"));
        assertEquals(0L, exec("def x = (char)4 def y = (float)1 return x & y"));
        assertEquals(0L, exec("def x = (int)4 def y = (float)1 return x & y"));
        assertEquals(0L, exec("def x = (long)4 def y = (float)1 return x & y"));
        assertEquals(0L, exec("def x = (float)4 def y = (float)1 return x & y"));
        assertEquals(0L, exec("def x = (double)4 def y = (float)1 return x & y"));

        assertEquals(0L, exec("def x = (byte)4 def y = (double)1 return x & y"));
        assertEquals(0L, exec("def x = (short)4 def y = (double)1 return x & y"));
        assertEquals(0L, exec("def x = (char)4 def y = (double)1 return x & y"));
        assertEquals(0L, exec("def x = (int)4 def y = (double)1 return x & y"));
        assertEquals(0L, exec("def x = (long)4 def y = (double)1 return x & y"));
        assertEquals(0L, exec("def x = (float)4 def y = (double)1 return x & y"));
        assertEquals(0L, exec("def x = (double)4 def y = (double)1 return x & y"));

        assertEquals(0, exec("def x = (Byte)4 def y = (byte)1 return x & y"));
        assertEquals(0, exec("def x = (Short)4 def y = (short)1 return x & y"));
        assertEquals(0, exec("def x = (Character)4 def y = (char)1 return x & y"));
        assertEquals(0, exec("def x = (Integer)4 def y = (int)1 return x & y"));
        assertEquals(0L, exec("def x = (Long)4 def y = (long)1 return x & y"));
        assertEquals(0L, exec("def x = (Float)4 def y = (float)1 return x & y"));
        assertEquals(0L, exec("def x = (Double)4 def y = (double)1 return x & y"));
    }

    public void testXor() {
        assertEquals(5, exec("def x = (byte)4 def y = (byte)1 return x ^ y"));
        assertEquals(5, exec("def x = (short)4 def y = (byte)1 return x ^ y"));
        assertEquals(5, exec("def x = (char)4 def y = (byte)1 return x ^ y"));
        assertEquals(5, exec("def x = (int)4 def y = (byte)1 return x ^ y"));
        assertEquals(5L, exec("def x = (long)4 def y = (byte)1 return x ^ y"));
        assertEquals(5L, exec("def x = (float)4 def y = (byte)1 return x ^ y"));
        assertEquals(5L, exec("def x = (double)4 def y = (byte)1 return x ^ y"));

        assertEquals(5, exec("def x = (byte)4 def y = (short)1 return x ^ y"));
        assertEquals(5, exec("def x = (short)4 def y = (short)1 return x ^ y"));
        assertEquals(5, exec("def x = (char)4 def y = (short)1 return x ^ y"));
        assertEquals(5, exec("def x = (int)4 def y = (short)1 return x ^ y"));
        assertEquals(5L, exec("def x = (long)4 def y = (short)1 return x ^ y"));
        assertEquals(5L, exec("def x = (float)4 def y = (short)1 return x ^ y"));
        assertEquals(5L, exec("def x = (double)4 def y = (short)1 return x ^ y"));

        assertEquals(5, exec("def x = (byte)4 def y = (char)1 return x ^ y"));
        assertEquals(5, exec("def x = (short)4 def y = (char)1 return x ^ y"));
        assertEquals(5, exec("def x = (char)4 def y = (char)1 return x ^ y"));
        assertEquals(5, exec("def x = (int)4 def y = (char)1 return x ^ y"));
        assertEquals(5L, exec("def x = (long)4 def y = (char)1 return x ^ y"));
        assertEquals(5L, exec("def x = (float)4 def y = (char)1 return x ^ y"));
        assertEquals(5L, exec("def x = (double)4 def y = (char)1 return x ^ y"));

        assertEquals(5, exec("def x = (byte)4 def y = (int)1 return x ^ y"));
        assertEquals(5, exec("def x = (short)4 def y = (int)1 return x ^ y"));
        assertEquals(5, exec("def x = (char)4 def y = (int)1 return x ^ y"));
        assertEquals(5, exec("def x = (int)4 def y = (int)1 return x ^ y"));
        assertEquals(5L, exec("def x = (long)4 def y = (int)1 return x ^ y"));
        assertEquals(5L, exec("def x = (float)4 def y = (int)1 return x ^ y"));
        assertEquals(5L, exec("def x = (double)4 def y = (int)1 return x ^ y"));

        assertEquals(5L, exec("def x = (byte)4 def y = (long)1 return x ^ y"));
        assertEquals(5L, exec("def x = (short)4 def y = (long)1 return x ^ y"));
        assertEquals(5L, exec("def x = (char)4 def y = (long)1 return x ^ y"));
        assertEquals(5L, exec("def x = (int)4 def y = (long)1 return x ^ y"));
        assertEquals(5L, exec("def x = (long)4 def y = (long)1 return x ^ y"));
        assertEquals(5L, exec("def x = (float)4 def y = (long)1 return x ^ y"));
        assertEquals(5L, exec("def x = (double)4 def y = (long)1 return x ^ y"));

        assertEquals(5L, exec("def x = (byte)4 def y = (float)1 return x ^ y"));
        assertEquals(5L, exec("def x = (short)4 def y = (float)1 return x ^ y"));
        assertEquals(5L, exec("def x = (char)4 def y = (float)1 return x ^ y"));
        assertEquals(5L, exec("def x = (int)4 def y = (float)1 return x ^ y"));
        assertEquals(5L, exec("def x = (long)4 def y = (float)1 return x ^ y"));
        assertEquals(5L, exec("def x = (float)4 def y = (float)1 return x ^ y"));
        assertEquals(5L, exec("def x = (double)4 def y = (float)1 return x ^ y"));

        assertEquals(5L, exec("def x = (byte)4 def y = (double)1 return x ^ y"));
        assertEquals(5L, exec("def x = (short)4 def y = (double)1 return x ^ y"));
        assertEquals(5L, exec("def x = (char)4 def y = (double)1 return x ^ y"));
        assertEquals(5L, exec("def x = (int)4 def y = (double)1 return x ^ y"));
        assertEquals(5L, exec("def x = (long)4 def y = (double)1 return x ^ y"));
        assertEquals(5L, exec("def x = (float)4 def y = (double)1 return x ^ y"));
        assertEquals(5L, exec("def x = (double)4 def y = (double)1 return x ^ y"));

        assertEquals(5, exec("def x = (Byte)4 def y = (byte)1 return x ^ y"));
        assertEquals(5, exec("def x = (Short)4 def y = (short)1 return x ^ y"));
        assertEquals(5, exec("def x = (Character)4 def y = (char)1 return x ^ y"));
        assertEquals(5, exec("def x = (Integer)4 def y = (int)1 return x ^ y"));
        assertEquals(5L, exec("def x = (Long)4 def y = (long)1 return x ^ y"));
        assertEquals(5L, exec("def x = (Float)4 def y = (float)1 return x ^ y"));
        assertEquals(5L, exec("def x = (Double)4 def y = (double)1 return x ^ y"));
    }

    public void testOr() {
        assertEquals(5, exec("def x = (byte)4 def y = (byte)1 return x | y"));
        assertEquals(5, exec("def x = (short)4 def y = (byte)1 return x | y"));
        assertEquals(5, exec("def x = (char)4 def y = (byte)1 return x | y"));
        assertEquals(5, exec("def x = (int)4 def y = (byte)1 return x | y"));
        assertEquals(5L, exec("def x = (long)4 def y = (byte)1 return x | y"));
        assertEquals(5L, exec("def x = (float)4 def y = (byte)1 return x | y"));
        assertEquals(5L, exec("def x = (double)4 def y = (byte)1 return x | y"));

        assertEquals(5, exec("def x = (byte)4 def y = (short)1 return x | y"));
        assertEquals(5, exec("def x = (short)4 def y = (short)1 return x | y"));
        assertEquals(5, exec("def x = (char)4 def y = (short)1 return x | y"));
        assertEquals(5, exec("def x = (int)4 def y = (short)1 return x | y"));
        assertEquals(5L, exec("def x = (long)4 def y = (short)1 return x | y"));
        assertEquals(5L, exec("def x = (float)4 def y = (short)1 return x | y"));
        assertEquals(5L, exec("def x = (double)4 def y = (short)1 return x | y"));

        assertEquals(5, exec("def x = (byte)4 def y = (char)1 return x | y"));
        assertEquals(5, exec("def x = (short)4 def y = (char)1 return x | y"));
        assertEquals(5, exec("def x = (char)4 def y = (char)1 return x | y"));
        assertEquals(5, exec("def x = (int)4 def y = (char)1 return x | y"));
        assertEquals(5L, exec("def x = (long)4 def y = (char)1 return x | y"));
        assertEquals(5L, exec("def x = (float)4 def y = (char)1 return x | y"));
        assertEquals(5L, exec("def x = (double)4 def y = (char)1 return x | y"));

        assertEquals(5, exec("def x = (byte)4 def y = (int)1 return x | y"));
        assertEquals(5, exec("def x = (short)4 def y = (int)1 return x | y"));
        assertEquals(5, exec("def x = (char)4 def y = (int)1 return x | y"));
        assertEquals(5, exec("def x = (int)4 def y = (int)1 return x | y"));
        assertEquals(5L, exec("def x = (long)4 def y = (int)1 return x | y"));
        assertEquals(5L, exec("def x = (float)4 def y = (int)1 return x | y"));
        assertEquals(5L, exec("def x = (double)4 def y = (int)1 return x | y"));

        assertEquals(5L, exec("def x = (byte)4 def y = (long)1 return x | y"));
        assertEquals(5L, exec("def x = (short)4 def y = (long)1 return x | y"));
        assertEquals(5L, exec("def x = (char)4 def y = (long)1 return x | y"));
        assertEquals(5L, exec("def x = (int)4 def y = (long)1 return x | y"));
        assertEquals(5L, exec("def x = (long)4 def y = (long)1 return x | y"));
        assertEquals(5L, exec("def x = (float)4 def y = (long)1 return x | y"));
        assertEquals(5L, exec("def x = (double)4 def y = (long)1 return x | y"));

        assertEquals(5L, exec("def x = (byte)4 def y = (float)1 return x | y"));
        assertEquals(5L, exec("def x = (short)4 def y = (float)1 return x | y"));
        assertEquals(5L, exec("def x = (char)4 def y = (float)1 return x | y"));
        assertEquals(5L, exec("def x = (int)4 def y = (float)1 return x | y"));
        assertEquals(5L, exec("def x = (long)4 def y = (float)1 return x | y"));
        assertEquals(5L, exec("def x = (float)4 def y = (float)1 return x | y"));
        assertEquals(5L, exec("def x = (double)4 def y = (float)1 return x | y"));

        assertEquals(5L, exec("def x = (byte)4 def y = (double)1 return x | y"));
        assertEquals(5L, exec("def x = (short)4 def y = (double)1 return x | y"));
        assertEquals(5L, exec("def x = (char)4 def y = (double)1 return x | y"));
        assertEquals(5L, exec("def x = (int)4 def y = (double)1 return x | y"));
        assertEquals(5L, exec("def x = (long)4 def y = (double)1 return x | y"));
        assertEquals(5L, exec("def x = (float)4 def y = (double)1 return x | y"));
        assertEquals(5L, exec("def x = (double)4 def y = (double)1 return x | y"));

        assertEquals(5, exec("def x = (Byte)4 def y = (byte)1 return x | y"));
        assertEquals(5, exec("def x = (Short)4 def y = (short)1 return x | y"));
        assertEquals(5, exec("def x = (Character)4 def y = (char)1 return x | y"));
        assertEquals(5, exec("def x = (Integer)4 def y = (int)1 return x | y"));
        assertEquals(5L, exec("def x = (Long)4 def y = (long)1 return x | y"));
        assertEquals(5L, exec("def x = (Float)4 def y = (float)1 return x | y"));
        assertEquals(5L, exec("def x = (Double)4 def y = (double)1 return x | y"));
    }

    public void testEq() {
        assertEquals(true, exec("def x = (byte)7 def y = (int)7 return x == y"));
        assertEquals(true, exec("def x = (short)6 def y = (int)6 return x == y"));
        assertEquals(true, exec("def x = (char)5 def y = (int)5 return x == y"));
        assertEquals(true, exec("def x = (int)4 def y = (int)4 return x == y"));
        assertEquals(false, exec("def x = (long)5 def y = (int)3 return x == y"));
        assertEquals(false, exec("def x = (float)6 def y = (int)2 return x == y"));
        assertEquals(false, exec("def x = (double)7 def y = (int)1 return x == y"));

        assertEquals(true, exec("def x = (byte)7 def y = (double)7 return x == y"));
        assertEquals(true, exec("def x = (short)6 def y = (double)6 return x == y"));
        assertEquals(true, exec("def x = (char)5 def y = (double)5 return x == y"));
        assertEquals(true, exec("def x = (int)4 def y = (double)4 return x == y"));
        assertEquals(false, exec("def x = (long)5 def y = (double)3 return x == y"));
        assertEquals(false, exec("def x = (float)6 def y = (double)2 return x == y"));
        assertEquals(false, exec("def x = (double)7 def y = (double)1 return x == y"));

        assertEquals(true, exec("def x = new HashMap() def y = new HashMap() return x == y"));
        assertEquals(false, exec("def x = new HashMap() x.put(3, 3) def y = new HashMap() return x == y"));
        assertEquals(true, exec("def x = new HashMap() x.put(3, 3) def y = new HashMap() y.put(3, 3) return x == y"));
        assertEquals(true, exec("def x = new HashMap() def y = x x.put(3, 3) y.put(3, 3) return x == y"));
    }

    public void testEqr() {
        assertEquals(false, exec("def x = (byte)7 def y = (int)7 return x === y"));
        assertEquals(false, exec("def x = (short)6 def y = (int)6 return x === y"));
        assertEquals(false, exec("def x = (char)5 def y = (int)5 return x === y"));
        assertEquals(true, exec("def x = (int)4 def y = (int)4 return x === y"));
        assertEquals(false, exec("def x = (long)5 def y = (int)3 return x === y"));
        assertEquals(false, exec("def x = (float)6 def y = (int)2 return x === y"));
        assertEquals(false, exec("def x = (double)7 def y = (int)1 return x === y"));

        assertEquals(false, exec("def x = new HashMap() def y = new HashMap() return x === y"));
        assertEquals(false, exec("def x = new HashMap() x.put(3, 3) def y = new HashMap() return x === y"));
        assertEquals(false, exec("def x = new HashMap() x.put(3, 3) def y = new HashMap() y.put(3, 3) return x === y"));
        assertEquals(true, exec("def x = new HashMap() def y = x x.put(3, 3) y.put(3, 3) return x === y"));
    }

    public void testNe() {
        assertEquals(false, exec("def x = (byte)7 def y = (int)7 return x != y"));
        assertEquals(false, exec("def x = (short)6 def y = (int)6 return x != y"));
        assertEquals(false, exec("def x = (char)5 def y = (int)5 return x != y"));
        assertEquals(false, exec("def x = (int)4 def y = (int)4 return x != y"));
        assertEquals(true, exec("def x = (long)5 def y = (int)3 return x != y"));
        assertEquals(true, exec("def x = (float)6 def y = (int)2 return x != y"));
        assertEquals(true, exec("def x = (double)7 def y = (int)1 return x != y"));

        assertEquals(false, exec("def x = (byte)7 def y = (double)7 return x != y"));
        assertEquals(false, exec("def x = (short)6 def y = (double)6 return x != y"));
        assertEquals(false, exec("def x = (char)5 def y = (double)5 return x != y"));
        assertEquals(false, exec("def x = (int)4 def y = (double)4 return x != y"));
        assertEquals(true, exec("def x = (long)5 def y = (double)3 return x != y"));
        assertEquals(true, exec("def x = (float)6 def y = (double)2 return x != y"));
        assertEquals(true, exec("def x = (double)7 def y = (double)1 return x != y"));

        assertEquals(false, exec("def x = new HashMap() def y = new HashMap() return x != y"));
        assertEquals(true, exec("def x = new HashMap() x.put(3, 3) def y = new HashMap() return x != y"));
        assertEquals(false, exec("def x = new HashMap() x.put(3, 3) def y = new HashMap() y.put(3, 3) return x != y"));
        assertEquals(false, exec("def x = new HashMap() def y = x x.put(3, 3) y.put(3, 3) return x != y"));
    }

    public void testNer() {
        assertEquals(true, exec("def x = (byte)7 def y = (int)7 return x !== y"));
        assertEquals(true, exec("def x = (short)6 def y = (int)6 return x !== y"));
        assertEquals(true, exec("def x = (char)5 def y = (int)5 return x !== y"));
        assertEquals(false, exec("def x = (int)4 def y = (int)4 return x !== y"));
        assertEquals(true, exec("def x = (long)5 def y = (int)3 return x !== y"));
        assertEquals(true, exec("def x = (float)6 def y = (int)2 return x !== y"));
        assertEquals(true, exec("def x = (double)7 def y = (int)1 return x !== y"));

        assertEquals(true, exec("def x = new HashMap() def y = new HashMap() return x !== y"));
        assertEquals(true, exec("def x = new HashMap() x.put(3, 3) def y = new HashMap() return x !== y"));
        assertEquals(true, exec("def x = new HashMap() x.put(3, 3) def y = new HashMap() y.put(3, 3) return x !== y"));
        assertEquals(false, exec("def x = new HashMap() def y = x x.put(3, 3) y.put(3, 3) return x !== y"));
    }

    public void testLt() {
        assertEquals(true, exec("def x = (byte)1 def y = (int)7 return x < y"));
        assertEquals(true, exec("def x = (short)2 def y = (int)6 return x < y"));
        assertEquals(true, exec("def x = (char)3 def y = (int)5 return x < y"));
        assertEquals(false, exec("def x = (int)4 def y = (int)4 return x < y"));
        assertEquals(false, exec("def x = (long)5 def y = (int)3 return x < y"));
        assertEquals(false, exec("def x = (float)6 def y = (int)2 return x < y"));
        assertEquals(false, exec("def x = (double)7 def y = (int)1 return x < y"));

        assertEquals(true, exec("def x = (byte)1 def y = (double)7 return x < y"));
        assertEquals(true, exec("def x = (short)2 def y = (double)6 return x < y"));
        assertEquals(true, exec("def x = (char)3 def y = (double)5 return x < y"));
        assertEquals(false, exec("def x = (int)4 def y = (double)4 return x < y"));
        assertEquals(false, exec("def x = (long)5 def y = (double)3 return x < y"));
        assertEquals(false, exec("def x = (float)6 def y = (double)2 return x < y"));
        assertEquals(false, exec("def x = (double)7 def y = (double)1 return x < y"));
    }

    public void testLte() {
        assertEquals(true, exec("def x = (byte)1 def y = (int)7 return x <= y"));
        assertEquals(true, exec("def x = (short)2 def y = (int)6 return x <= y"));
        assertEquals(true, exec("def x = (char)3 def y = (int)5 return x <= y"));
        assertEquals(true, exec("def x = (int)4 def y = (int)4 return x <= y"));
        assertEquals(false, exec("def x = (long)5 def y = (int)3 return x <= y"));
        assertEquals(false, exec("def x = (float)6 def y = (int)2 return x <= y"));
        assertEquals(false, exec("def x = (double)7 def y = (int)1 return x <= y"));

        assertEquals(true, exec("def x = (byte)1 def y = (double)7 return x <= y"));
        assertEquals(true, exec("def x = (short)2 def y = (double)6 return x <= y"));
        assertEquals(true, exec("def x = (char)3 def y = (double)5 return x <= y"));
        assertEquals(true, exec("def x = (int)4 def y = (double)4 return x <= y"));
        assertEquals(false, exec("def x = (long)5 def y = (double)3 return x <= y"));
        assertEquals(false, exec("def x = (float)6 def y = (double)2 return x <= y"));
        assertEquals(false, exec("def x = (double)7 def y = (double)1 return x <= y"));
    }

    public void testGt() {
        assertEquals(false, exec("def x = (byte)1 def y = (int)7 return x > y"));
        assertEquals(false, exec("def x = (short)2 def y = (int)6 return x > y"));
        assertEquals(false, exec("def x = (char)3 def y = (int)5 return x > y"));
        assertEquals(false, exec("def x = (int)4 def y = (int)4 return x > y"));
        assertEquals(true, exec("def x = (long)5 def y = (int)3 return x > y"));
        assertEquals(true, exec("def x = (float)6 def y = (int)2 return x > y"));
        assertEquals(true, exec("def x = (double)7 def y = (int)1 return x > y"));

        assertEquals(false, exec("def x = (byte)1 def y = (double)7 return x > y"));
        assertEquals(false, exec("def x = (short)2 def y = (double)6 return x > y"));
        assertEquals(false, exec("def x = (char)3 def y = (double)5 return x > y"));
        assertEquals(false, exec("def x = (int)4 def y = (double)4 return x > y"));
        assertEquals(true, exec("def x = (long)5 def y = (double)3 return x > y"));
        assertEquals(true, exec("def x = (float)6 def y = (double)2 return x > y"));
        assertEquals(true, exec("def x = (double)7 def y = (double)1 return x > y"));
    }

    public void testGte() {
        assertEquals(false, exec("def x = (byte)1 def y = (int)7 return x >= y"));
        assertEquals(false, exec("def x = (short)2 def y = (int)6 return x >= y"));
        assertEquals(false, exec("def x = (char)3 def y = (int)5 return x >= y"));
        assertEquals(true, exec("def x = (int)4 def y = (int)4 return x >= y"));
        assertEquals(true, exec("def x = (long)5 def y = (int)3 return x >= y"));
        assertEquals(true, exec("def x = (float)6 def y = (int)2 return x >= y"));
        assertEquals(true, exec("def x = (double)7 def y = (int)1 return x >= y"));

        assertEquals(false, exec("def x = (byte)1 def y = (double)7 return x >= y"));
        assertEquals(false, exec("def x = (short)2 def y = (double)6 return x >= y"));
        assertEquals(false, exec("def x = (char)3 def y = (double)5 return x >= y"));
        assertEquals(true, exec("def x = (int)4 def y = (double)4 return x >= y"));
        assertEquals(true, exec("def x = (long)5 def y = (double)3 return x >= y"));
        assertEquals(true, exec("def x = (float)6 def y = (double)2 return x >= y"));
        assertEquals(true, exec("def x = (double)7 def y = (double)1 return x >= y"));
    }
}
