package org.elasticsearch.painless;

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

/** Tests method overloading */
public class OverloadTests extends ScriptTestCase {

    public void testMethod() {
        assertEquals(2, exec("return 'abc123abc'.indexOf('c');"));
        assertEquals(8, exec("return 'abc123abc'.indexOf('c', 3);"));
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("return 'abc123abc'.indexOf('c', 3, 'bogus');");
        });
        assertTrue(expected.getMessage().contains("[indexOf] with [3] arguments"));
    }
    
    public void testMethodDynamic() {
        assertEquals(2, exec("def x = 'abc123abc'; return x.indexOf('c');"));
        assertEquals(8, exec("def x = 'abc123abc'; return x.indexOf('c', 3);"));
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("def x = 'abc123abc'; return x.indexOf('c', 3, 'bogus');");
        });
        assertTrue(expected.getMessage().contains("dynamic method [indexOf]"));
    }
    
    public void testConstructor() {
        assertEquals(true, exec("org.elasticsearch.painless.FeatureTest f = new org.elasticsearch.painless.FeatureTest();" +
                                "return f.x == 0 && f.y == 0;"));
        assertEquals(true, exec("org.elasticsearch.painless.FeatureTest f = new org.elasticsearch.painless.FeatureTest(1, 2);" +
                                "return f.x == 1 && f.y == 2;"));
    }
    
    public void testStatic() {
        assertEquals(true, exec("return org.elasticsearch.painless.FeatureTest.overloadedStatic();"));
        assertEquals(false, exec("return org.elasticsearch.painless.FeatureTest.overloadedStatic(false);"));
    }
}
