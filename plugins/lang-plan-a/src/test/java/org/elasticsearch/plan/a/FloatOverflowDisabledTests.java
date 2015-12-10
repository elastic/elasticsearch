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

/** Tests floating point overflow with numeric overflow disabled */
public class FloatOverflowDisabledTests extends ScriptTestCase {
    
    @Override
    protected Settings getSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(super.getSettings());
        builder.put(PlanAScriptEngineService.NUMERIC_OVERFLOW, false);
        return builder.build();
    }

    public void testAssignmentAdditionOverflow() {        
        // float
        try {
            exec("float x = 3.4028234663852886E38f; x += 3.4028234663852886E38f; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("float x = -3.4028234663852886E38f; x += -3.4028234663852886E38f; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        
        // double
        try {
            exec("double x = 1.7976931348623157E308; x += 1.7976931348623157E308; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("double x = -1.7976931348623157E308; x += -1.7976931348623157E308; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testAssignmentSubtractionOverflow() {    
        // float
        try {
            exec("float x = 3.4028234663852886E38f; x -= -3.4028234663852886E38f; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("float x = -3.4028234663852886E38f; x -= 3.4028234663852886E38f; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        
        // double
        try {
            exec("double x = 1.7976931348623157E308; x -= -1.7976931348623157E308; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("double x = -1.7976931348623157E308; x -= 1.7976931348623157E308; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testAssignmentMultiplicationOverflow() {
        // float
        try {
            exec("float x = 3.4028234663852886E38f; x *= 3.4028234663852886E38f; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("float x = 3.4028234663852886E38f; x *= -3.4028234663852886E38f; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        
        // double
        try {
            exec("double x = 1.7976931348623157E308; x *= 1.7976931348623157E308; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("double x = 1.7976931348623157E308; x *= -1.7976931348623157E308; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testAssignmentDivisionOverflow() {
        // float
        try {
            exec("float x = 3.4028234663852886E38f; x /= 1.401298464324817E-45f; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("float x = 3.4028234663852886E38f; x /= -1.401298464324817E-45f; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("float x = 1.0f; x /= 0.0f; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        
        // double
        try {
            exec("double x = 1.7976931348623157E308; x /= 4.9E-324; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("double x = 1.7976931348623157E308; x /= -4.9E-324; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("double x = 1.0f; x /= 0.0; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }

    public void testAddition() throws Exception {
        try {
            exec("float x = 3.4028234663852886E38f; float y = 3.4028234663852886E38f; return x + y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("double x = 1.7976931348623157E308; double y = 1.7976931348623157E308; return x + y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testAdditionConst() throws Exception {
        try {
            exec("return 3.4028234663852886E38f + 3.4028234663852886E38f;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("return 1.7976931348623157E308 + 1.7976931348623157E308;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testSubtraction() throws Exception {
        try {
            exec("float x = -3.4028234663852886E38f; float y = 3.4028234663852886E38f; return x - y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("double x = -1.7976931348623157E308; double y = 1.7976931348623157E308; return x - y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testSubtractionConst() throws Exception {
        try {
            exec("return -3.4028234663852886E38f - 3.4028234663852886E38f;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("return -1.7976931348623157E308 - 1.7976931348623157E308;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testMultiplication() throws Exception {
        try {
            exec("float x = 3.4028234663852886E38f; float y = 3.4028234663852886E38f; return x * y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("double x = 1.7976931348623157E308; double y = 1.7976931348623157E308; return x * y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testMultiplicationConst() throws Exception {
        try {
            exec("return 3.4028234663852886E38f * 3.4028234663852886E38f;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("return 1.7976931348623157E308 * 1.7976931348623157E308;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }

    public void testDivision() throws Exception {
        try {
            exec("float x = 3.4028234663852886E38f; float y = 1.401298464324817E-45f; return x / y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("float x = 1.0f; float y = 0.0f; return x / y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("double x = 1.7976931348623157E308; double y = 4.9E-324; return x / y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("double x = 1.0; double y = 0.0; return x / y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testDivisionConst() throws Exception {
        try {
            exec("return 3.4028234663852886E38f / 1.401298464324817E-45f;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("return 1.0f / 0.0f;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("return 1.7976931348623157E308 / 4.9E-324;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("return 1.0 / 0.0;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testDivisionNaN() throws Exception {
        // float division, constant division, and assignment
        try {
            exec("float x = 0f; float y = 0f; return x / y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("return 0f / 0f;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("float x = 0f; x /= 0f; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        
        // double division, constant division, and assignment
        try {
            exec("double x = 0.0; double y = 0.0; return x / y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("return 0.0 / 0.0;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("double x = 0.0; x /= 0.0; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }
    
    public void testRemainderNaN() throws Exception {
        // float division, constant division, and assignment
        try {
            exec("float x = 1f; float y = 0f; return x % y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("return 1f % 0f;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("float x = 1f; x %= 0f; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        
        // double division, constant division, and assignment
        try {
            exec("double x = 1.0; double y = 0.0; return x % y;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("return 1.0 % 0.0;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
        try {
            exec("double x = 1.0; x %= 0.0; return x;");
            fail("didn't hit expected exception");
        } catch (ArithmeticException expected) {}
    }
}
