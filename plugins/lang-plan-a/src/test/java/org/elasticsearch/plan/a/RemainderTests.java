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

/** Tests for division operator across all types */
//TODO: NaN/Inf/overflow/...
public class RemainderTests extends ScriptTestCase {
    
    // TODO: byte,short,char
    
    public void testInt() throws Exception {
        assertEquals(1%1, exec("int x = 1; int y = 1; return x%y;"));
        assertEquals(2%3, exec("int x = 2; int y = 3; return x%y;"));
        assertEquals(5%10, exec("int x = 5; int y = 10; return x%y;"));
        assertEquals(10%1%2, exec("int x = 10; int y = 1; int z = 2; return x%y%z;"));
        assertEquals((10%1)%2, exec("int x = 10; int y = 1; int z = 2; return (x%y)%z;"));
        assertEquals(10%(4%3), exec("int x = 10; int y = 4; int z = 3; return x%(y%z);"));
        assertEquals(10%1, exec("int x = 10; int y = 1; return x%y;"));
        assertEquals(0%1, exec("int x = 0; int y = 1; return x%y;"));
    }
    
    public void testIntConst() throws Exception {
        assertEquals(1%1, exec("return 1%1;"));
        assertEquals(2%3, exec("return 2%3;"));
        assertEquals(5%10, exec("return 5%10;"));
        assertEquals(10%1%2, exec("return 10%1%2;"));
        assertEquals((10%1)%2, exec("return (10%1)%2;"));
        assertEquals(10%(4%3), exec("return 10%(4%3);"));
        assertEquals(10%1, exec("return 10%1;"));
        assertEquals(0%1, exec("return 0%1;"));
    }
    
    public void testLong() throws Exception {
        assertEquals(1L%1L, exec("long x = 1; long y = 1; return x%y;"));
        assertEquals(2L%3L, exec("long x = 2; long y = 3; return x%y;"));
        assertEquals(5L%10L, exec("long x = 5; long y = 10; return x%y;"));
        assertEquals(10L%1L%2L, exec("long x = 10; long y = 1; long z = 2; return x%y%z;"));
        assertEquals((10L%1L)%2L, exec("long x = 10; long y = 1; long z = 2; return (x%y)%z;"));
        assertEquals(10L%(4L%3L), exec("long x = 10; long y = 4; long z = 3; return x%(y%z);"));
        assertEquals(10L%1L, exec("long x = 10; long y = 1; return x%y;"));
        assertEquals(0L%1L, exec("long x = 0; long y = 1; return x%y;"));
    }
    
    public void testLongConst() throws Exception {
        assertEquals(1L%1L, exec("return 1L%1L;"));
        assertEquals(2L%3L, exec("return 2L%3L;"));
        assertEquals(5L%10L, exec("return 5L%10L;"));
        assertEquals(10L%1L%2L, exec("return 10L%1L%2L;"));
        assertEquals((10L%1L)%2L, exec("return (10L%1L)%2L;"));
        assertEquals(10L%(4L%3L), exec("return 10L%(4L%3L);"));
        assertEquals(10L%1L, exec("return 10L%1L;"));
        assertEquals(0L%1L, exec("return 0L%1L;"));
    }
    
    public void testFloat() throws Exception {
        assertEquals(1F%1F, exec("float x = 1; float y = 1; return x%y;"));
        assertEquals(2F%3F, exec("float x = 2; float y = 3; return x%y;"));
        assertEquals(5F%10F, exec("float x = 5; float y = 10; return x%y;"));
        assertEquals(10F%1F%2F, exec("float x = 10; float y = 1; float z = 2; return x%y%z;"));
        assertEquals((10F%1F)%2F, exec("float x = 10; float y = 1; float z = 2; return (x%y)%z;"));
        assertEquals(10F%(4F%3F), exec("float x = 10; float y = 4; float z = 3; return x%(y%z);"));
        assertEquals(10F%1F, exec("float x = 10; float y = 1; return x%y;"));
        assertEquals(0F%1F, exec("float x = 0; float y = 1; return x%y;"));
    }
    
    public void testFloatConst() throws Exception {
        assertEquals(1F%1F, exec("return 1F%1F;"));
        assertEquals(2F%3F, exec("return 2F%3F;"));
        assertEquals(5F%10F, exec("return 5F%10F;"));
        assertEquals(10F%1F%2F, exec("return 10F%1F%2F;"));
        assertEquals((10F%1F)%2F, exec("return (10F%1F)%2F;"));
        assertEquals(10F%(4F%3F), exec("return 10F%(4F%3F);"));
        assertEquals(10F%1F, exec("return 10F%1F;"));
        assertEquals(0F%1F, exec("return 0F%1F;"));
    }
    
    public void testDouble() throws Exception {
        assertEquals(1.0%1.0, exec("double x = 1; double y = 1; return x%y;"));
        assertEquals(2.0%3.0, exec("double x = 2; double y = 3; return x%y;"));
        assertEquals(5.0%10.0, exec("double x = 5; double y = 10; return x%y;"));
        assertEquals(10.0%1.0%2.0, exec("double x = 10; double y = 1; double z = 2; return x%y%z;"));
        assertEquals((10.0%1.0)%2.0, exec("double x = 10; double y = 1; double z = 2; return (x%y)%z;"));
        assertEquals(10.0%(4.0%3.0), exec("double x = 10; double y = 4; double z = 3; return x%(y%z);"));
        assertEquals(10.0%1.0, exec("double x = 10; double y = 1; return x%y;"));
        assertEquals(0.0%1.0, exec("double x = 0; double y = 1; return x%y;"));
    }
    
    public void testDoubleConst() throws Exception {
        assertEquals(1.0%1.0, exec("return 1.0%1.0;"));
        assertEquals(2.0%3.0, exec("return 2.0%3.0;"));
        assertEquals(5.0%10.0, exec("return 5.0%10.0;"));
        assertEquals(10.0%1.0%2.0, exec("return 10.0%1.0%2.0;"));
        assertEquals((10.0%1.0)%2.0, exec("return (10.0%1.0)%2.0;"));
        assertEquals(10.0%(4.0%3.0), exec("return 10.0%(4.0%3.0);"));
        assertEquals(10.0%1.0, exec("return 10.0%1.0;"));
        assertEquals(0.0%1.0, exec("return 0.0%1.0;"));
    }
    
    public void testDivideByZero() throws Exception {
        try {
            exec("int x = 1; int y = 0; return x % y;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {
            // divide by zero
        }
        
        try {
            exec("long x = 1L; long y = 0L; return x % y;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {
            // divide by zero
        }
    }
    
    public void testDivideByZeroConst() throws Exception {
        try {
            exec("return 1%0;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {
            // divide by zero
        }
        
        try {
            exec("return 1L%0L;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {
            // divide by zero
        }
    }
}
