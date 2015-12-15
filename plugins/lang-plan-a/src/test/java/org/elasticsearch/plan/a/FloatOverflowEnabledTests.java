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

/** Tests floating point overflow with numeric overflow enabled */
public class FloatOverflowEnabledTests extends ScriptTestCase {
    
    @Override
    protected Settings getSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(super.getSettings());
        builder.put(PlanAScriptEngineService.NUMERIC_OVERFLOW, true);
        return builder.build();
    }

    public void testAssignmentAdditionOverflow() {        
        // float
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; x += 3.4028234663852886E38f; return x;"));
        assertEquals(Float.NEGATIVE_INFINITY, exec("float x = -3.4028234663852886E38f; x += -3.4028234663852886E38f; return x;"));
        
        // double
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; x += 1.7976931348623157E308; return x;"));
        assertEquals(Double.NEGATIVE_INFINITY, exec("double x = -1.7976931348623157E308; x += -1.7976931348623157E308; return x;"));
    }
    
    public void testAssignmentSubtractionOverflow() {    
        // float
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; x -= -3.4028234663852886E38f; return x;"));
        assertEquals(Float.NEGATIVE_INFINITY, exec("float x = -3.4028234663852886E38f; x -= 3.4028234663852886E38f; return x;"));
        
        // double
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; x -= -1.7976931348623157E308; return x;"));
        assertEquals(Double.NEGATIVE_INFINITY, exec("double x = -1.7976931348623157E308; x -= 1.7976931348623157E308; return x;"));
    }
    
    public void testAssignmentMultiplicationOverflow() {
        // float
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; x *= 3.4028234663852886E38f; return x;"));
        assertEquals(Float.NEGATIVE_INFINITY, exec("float x = 3.4028234663852886E38f; x *= -3.4028234663852886E38f; return x;"));
        
        // double
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; x *= 1.7976931348623157E308; return x;"));
        assertEquals(Double.NEGATIVE_INFINITY, exec("double x = 1.7976931348623157E308; x *= -1.7976931348623157E308; return x;"));
    }
    
    public void testAssignmentDivisionOverflow() {
        // float
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; x /= 1.401298464324817E-45f; return x;"));
        assertEquals(Float.NEGATIVE_INFINITY, exec("float x = 3.4028234663852886E38f; x /= -1.401298464324817E-45f; return x;"));
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 1.0f; x /= 0.0f; return x;"));
        
        // double
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; x /= 4.9E-324; return x;"));
        assertEquals(Double.NEGATIVE_INFINITY, exec("double x = 1.7976931348623157E308; x /= -4.9E-324; return x;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.0f; x /= 0.0; return x;"));
    }

    public void testAddition() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; float y = 3.4028234663852886E38f; return x + y;"));        
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; double y = 1.7976931348623157E308; return x + y;"));
    }
    
    public void testAdditionConst() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, exec("return 3.4028234663852886E38f + 3.4028234663852886E38f;"));        
        assertEquals(Double.POSITIVE_INFINITY, exec("return 1.7976931348623157E308 + 1.7976931348623157E308;"));
    }
    
    public void testSubtraction() throws Exception {
        assertEquals(Float.NEGATIVE_INFINITY, exec("float x = -3.4028234663852886E38f; float y = 3.4028234663852886E38f; return x - y;"));        
        assertEquals(Double.NEGATIVE_INFINITY, exec("double x = -1.7976931348623157E308; double y = 1.7976931348623157E308; return x - y;"));
    }
    
    public void testSubtractionConst() throws Exception {
        assertEquals(Float.NEGATIVE_INFINITY, exec("return -3.4028234663852886E38f - 3.4028234663852886E38f;"));        
        assertEquals(Double.NEGATIVE_INFINITY, exec("return -1.7976931348623157E308 - 1.7976931348623157E308;"));
    }
    
    public void testMultiplication() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; float y = 3.4028234663852886E38f; return x * y;"));        
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; double y = 1.7976931348623157E308; return x * y;"));
    }
    
    public void testMultiplicationConst() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, exec("return 3.4028234663852886E38f * 3.4028234663852886E38f;"));        
        assertEquals(Double.POSITIVE_INFINITY, exec("return 1.7976931348623157E308 * 1.7976931348623157E308;"));
    }

    public void testDivision() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; float y = 1.401298464324817E-45f; return x / y;"));
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 1.0f; float y = 0.0f; return x / y;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; double y = 4.9E-324; return x / y;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.0; double y = 0.0; return x / y;"));
    }
    
    public void testDivisionConst() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, exec("return 3.4028234663852886E38f / 1.401298464324817E-45f;"));
        assertEquals(Float.POSITIVE_INFINITY, exec("return 1.0f / 0.0f;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("return 1.7976931348623157E308 / 4.9E-324;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("return 1.0 / 0.0;"));
    }
    
    public void testDivisionNaN() throws Exception {
        // float division, constant division, and assignment
        assertTrue(Float.isNaN((Float) exec("float x = 0f; float y = 0f; return x / y;")));
        assertTrue(Float.isNaN((Float) exec("return 0f / 0f;")));
        assertTrue(Float.isNaN((Float) exec("float x = 0f; x /= 0f; return x;")));
        
        // double division, constant division, and assignment
        assertTrue(Double.isNaN((Double) exec("double x = 0.0; double y = 0.0; return x / y;")));
        assertTrue(Double.isNaN((Double) exec("return 0.0 / 0.0;")));
        assertTrue(Double.isNaN((Double) exec("double x = 0.0; x /= 0.0; return x;")));
    }
    
    public void testRemainderNaN() throws Exception {
        // float division, constant division, and assignment
        assertTrue(Float.isNaN((Float) exec("float x = 1f; float y = 0f; return x % y;")));
        assertTrue(Float.isNaN((Float) exec("return 1f % 0f;")));
        assertTrue(Float.isNaN((Float) exec("float x = 1f; x %= 0f; return x;")));
        
        // double division, constant division, and assignment
        assertTrue(Double.isNaN((Double) exec("double x = 1.0; double y = 0.0; return x % y;")));
        assertTrue(Double.isNaN((Double) exec("return 1.0 % 0.0;")));
        assertTrue(Double.isNaN((Double) exec("double x = 1.0; x %= 0.0; return x;")));
    }
}
