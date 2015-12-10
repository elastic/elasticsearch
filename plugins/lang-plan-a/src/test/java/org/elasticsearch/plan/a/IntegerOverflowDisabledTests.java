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

import org.elasticsearch.common.settings.Settings;

/** Tests integer overflow with numeric overflow disabled */
public class IntegerOverflowDisabledTests extends ScriptTestCase {
    
    @Override
    protected Settings getSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(super.getSettings());
        builder.put(PlanAScriptEngineService.NUMERIC_OVERFLOW, false);
        return builder.build();
    }

    public void testAssignmentAdditionOverflow() {
        // byte
        try {
            exec("byte x = 0; x += 128; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("byte x = 0; x += -129; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // short
        try {
            exec("short x = 0; x += 32768; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("byte x = 0; x += -32769; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // char
        try {
            exec("char x = 0; x += 65536; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("char x = 0; x += -65536; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // int
        try {
            exec("int x = 1; x += 2147483647; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("int x = -2; x += -2147483647; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // long
        try {
            exec("long x = 1; x += 9223372036854775807L; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("long x = -2; x += -9223372036854775807L; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testAssignmentSubtractionOverflow() {
        // byte
        try {
            exec("byte x = 0; x -= -128; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("byte x = 0; x -= 129; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // short
        try {
            exec("short x = 0; x -= -32768; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("byte x = 0; x -= 32769; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // char
        try {
            exec("char x = 0; x -= -65536; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("char x = 0; x -= 65536; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // int
        try {
            exec("int x = 1; x -= -2147483647; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("int x = -2; x -= 2147483647; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // long
        try {
            exec("long x = 1; x -= -9223372036854775807L; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("long x = -2; x -= 9223372036854775807L; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testAssignmentMultiplicationOverflow() {
        // byte
        try {
            exec("byte x = 2; x *= 128; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("byte x = 2; x *= -128; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // char
        try {
            exec("char x = 2; x *= 65536; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("char x = 2; x *= -65536; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // int
        try {
            exec("int x = 2; x *= 2147483647; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("int x = 2; x *= -2147483647; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // long
        try {
            exec("long x = 2; x *= 9223372036854775807L; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("long x = 2; x *= -9223372036854775807L; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testAssignmentDivisionOverflow() {
        // byte
        try {
            exec("byte x = (byte) -128; x /= -1; return x;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}

        // short
        try {
            exec("short x = (short) -32768; x /= -1; return x;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
        
        // cannot happen for char: unsigned
        
        // int
        try {
            exec("int x = -2147483647 - 1; x /= -1; return x;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
        
        // long
        try {
            exec("long x = -9223372036854775807L - 1L; x /=-1L; return x;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testIncrementOverFlow() throws Exception {
        // byte
        try {
            exec("byte x = 127; ++x; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("byte x = 127; x++; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("byte x = (byte) -128; --x; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("byte x = (byte) -128; x--; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // short
        try {
            exec("short x = 32767; ++x; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("short x = 32767; x++; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("short x = (short) -32768; --x; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("short x = (short) -32768; x--; return x;");
        } catch (ArithmeticException expected) {}
        
        // char
        try {
            exec("char x = 65535; ++x; return x;");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("char x = 65535; x++; return x;");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("char x = (char) 0; --x; return x;");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("char x = (char) 0; x--; return x;");
        } catch (ArithmeticException expected) {}
        
        // int
        try {
            exec("int x = 2147483647; ++x; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("int x = 2147483647; x++; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("int x = (int) -2147483648L; --x; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("int x = (int) -2147483648L; x--; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        // long
        try {
            exec("long x = 9223372036854775807L; ++x; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("long x = 9223372036854775807L; x++; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("long x = -9223372036854775807L - 1L; --x; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("long x = -9223372036854775807L - 1L; x--; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testAddition() throws Exception {
        try {
            exec("int x = 2147483647; int y = 2147483647; return x + y;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("long x = 9223372036854775807L; long y = 9223372036854775807L; return x + y;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testAdditionConst() throws Exception {
        try {
            exec("return 2147483647 + 2147483647;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("return 9223372036854775807L + 9223372036854775807L;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
    }
    
    
    public void testSubtraction() throws Exception {
        try {
            exec("int x = -10; int y = 2147483647; return x - y;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("long x = -10L; long y = 9223372036854775807L; return x - y;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testSubtractionConst() throws Exception {
        try {
            exec("return -10 - 2147483647;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("return -10L - 9223372036854775807L;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testMultiplication() throws Exception {
        try {
            exec("int x = 2147483647; int y = 2147483647; return x * y;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("long x = 9223372036854775807L; long y = 9223372036854775807L; return x * y;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testMultiplicationConst() throws Exception {
        try {
            exec("return 2147483647 * 2147483647;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("return 9223372036854775807L * 9223372036854775807L;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
    }

    public void testDivision() throws Exception {
        try {
            exec("int x = -2147483647 - 1; int y = -1; return x / y;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("long x = -9223372036854775808L; long y = -1L; return x / y;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testDivisionConst() throws Exception {
        try {
            exec("return (-2147483648) / -1;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}

        try {
            exec("return (-9223372036854775808L) / -1L;");
            fail("should have hit exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testNegationOverflow() throws Exception {
        try {
            exec("int x = -2147483648; x = -x; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("long x = -9223372036854775808L; x = -x; return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testNegationOverflowConst() throws Exception {
        try {
            exec("int x = -(-2147483648); return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
        
        try {
            exec("long x = -(-9223372036854775808L); return x;");
            fail("did not get expected exception");
        } catch (ArithmeticException expected) {}
    }
}
