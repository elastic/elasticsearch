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

import org.elasticsearch.test.ESTestCase;

/**
 * Tests utility methods (typically built-ins)
 */
public class UtilityTests extends ESTestCase {
    
    public void testDivideWithoutOverflowInt() {
        assertEquals(5 / 2, Utility.divideWithoutOverflow(5, 2));

        try {
            Utility.divideWithoutOverflow(Integer.MIN_VALUE, -1);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.divideWithoutOverflow(5, 0);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testDivideWithoutOverflowLong() {
        assertEquals(5L / 2L, Utility.divideWithoutOverflow(5L, 2L));
        
        try {
            Utility.divideWithoutOverflow(Long.MIN_VALUE, -1L);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.divideWithoutOverflow(5L, 0L);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testToByteExact() {
        for (int b = Byte.MIN_VALUE; b < Byte.MAX_VALUE; b++) {
            assertEquals((byte)b, Utility.toByteExact(b));
        }
        
        try {
            Utility.toByteExact(Byte.MIN_VALUE - 1);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.toByteExact(Byte.MAX_VALUE + 1);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testToShortExact() {
        for (int s = Short.MIN_VALUE; s < Short.MAX_VALUE; s++) {
            assertEquals((short)s, Utility.toShortExact(s));
        }
        
        try {
            Utility.toShortExact(Short.MIN_VALUE - 1);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.toShortExact(Short.MAX_VALUE + 1);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testToCharExact() {
        for (int c = Character.MIN_VALUE; c < Character.MAX_VALUE; c++) {
            assertEquals((char)c, Utility.toCharExact(c));
        }
        
        try {
            Utility.toCharExact(Character.MIN_VALUE - 1);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.toCharExact(Character.MAX_VALUE + 1);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testAddWithoutOverflowFloat() {
        assertEquals(10F, Utility.addWithoutOverflow(5F, 5F), 0F);
        assertTrue(Float.isNaN(Utility.addWithoutOverflow(5F, Float.NaN)));
        assertTrue(Float.isNaN(Utility.addWithoutOverflow(Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY)));

        try {
            Utility.addWithoutOverflow(Float.MAX_VALUE, Float.MAX_VALUE);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.addWithoutOverflow(-Float.MAX_VALUE, -Float.MAX_VALUE);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testAddWithoutOverflowDouble() {
        assertEquals(10D, Utility.addWithoutOverflow(5D, 5D), 0D);
        assertTrue(Double.isNaN(Utility.addWithoutOverflow(5D, Double.NaN)));
        assertTrue(Double.isNaN(Utility.addWithoutOverflow(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)));
        
        try {
            Utility.addWithoutOverflow(Double.MAX_VALUE, Double.MAX_VALUE);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.addWithoutOverflow(-Double.MAX_VALUE, -Double.MAX_VALUE);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testSubtractWithoutOverflowFloat() {
        assertEquals(5F, Utility.subtractWithoutOverflow(10F, 5F), 0F);
        assertTrue(Float.isNaN(Utility.subtractWithoutOverflow(5F, Float.NaN)));
        assertTrue(Float.isNaN(Utility.subtractWithoutOverflow(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY)));

        try {
            Utility.subtractWithoutOverflow(Float.MAX_VALUE, -Float.MAX_VALUE);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.subtractWithoutOverflow(-Float.MAX_VALUE, Float.MAX_VALUE);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testSubtractWithoutOverflowDouble() {
        assertEquals(5D, Utility.subtractWithoutOverflow(10D, 5D), 0D);
        assertTrue(Double.isNaN(Utility.subtractWithoutOverflow(5D, Double.NaN)));
        assertTrue(Double.isNaN(Utility.subtractWithoutOverflow(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY)));
        
        try {
            Utility.subtractWithoutOverflow(Double.MAX_VALUE, -Double.MAX_VALUE);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.subtractWithoutOverflow(-Double.MAX_VALUE, Double.MAX_VALUE);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testMultiplyWithoutOverflowFloat() {
        assertEquals(25F, Utility.multiplyWithoutOverflow(5F, 5F), 0F);
        assertTrue(Float.isNaN(Utility.multiplyWithoutOverflow(5F, Float.NaN)));
        assertEquals(Float.POSITIVE_INFINITY, Utility.multiplyWithoutOverflow(5F, Float.POSITIVE_INFINITY), 0F);

        try {
            Utility.multiplyWithoutOverflow(Float.MAX_VALUE, Float.MAX_VALUE);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testMultiplyWithoutOverflowDouble() {
        assertEquals(25D, Utility.multiplyWithoutOverflow(5D, 5D), 0D);
        assertTrue(Double.isNaN(Utility.multiplyWithoutOverflow(5D, Double.NaN)));
        assertEquals(Double.POSITIVE_INFINITY, Utility.multiplyWithoutOverflow(5D, Double.POSITIVE_INFINITY), 0D);
        
        try {
            Utility.multiplyWithoutOverflow(Double.MAX_VALUE, Double.MAX_VALUE);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testDivideWithoutOverflowFloat() {
        assertEquals(5F, Utility.divideWithoutOverflow(25F, 5F), 0F);
        assertTrue(Float.isNaN(Utility.divideWithoutOverflow(5F, Float.NaN)));
        assertEquals(Float.POSITIVE_INFINITY, Utility.divideWithoutOverflow(Float.POSITIVE_INFINITY, 5F), 0F);

        try {
            Utility.divideWithoutOverflow(Float.MAX_VALUE, Float.MIN_VALUE);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.divideWithoutOverflow(0F, 0F);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.divideWithoutOverflow(5F, 0F);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testDivideWithoutOverflowDouble() {
        assertEquals(5D, Utility.divideWithoutOverflow(25D, 5D), 0D);
        assertTrue(Double.isNaN(Utility.divideWithoutOverflow(5D, Double.NaN)));
        assertEquals(Double.POSITIVE_INFINITY, Utility.divideWithoutOverflow(Double.POSITIVE_INFINITY, 5D), 0D);
        
        try {
            Utility.divideWithoutOverflow(Double.MAX_VALUE, Double.MIN_VALUE);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.divideWithoutOverflow(0D, 0D);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            Utility.divideWithoutOverflow(5D, 0D);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testRemainderWithoutOverflowFloat() {
        assertEquals(1F, Utility.remainderWithoutOverflow(25F, 4F), 0F);
        
        try {
            Utility.remainderWithoutOverflow(5F, 0F);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testRemainderWithoutOverflowDouble() {
        assertEquals(1D, Utility.remainderWithoutOverflow(25D, 4D), 0D);
        
        try {
            Utility.remainderWithoutOverflow(5D, 0D);
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
}
