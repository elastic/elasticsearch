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

public class FunctionRefTests extends ScriptTestCase {

    public void testStaticMethodReference() {
        assertEquals(1, exec("List l = new ArrayList(); l.add(2); l.add(1); l.sort(Integer::compare); return l.get(0);"));
    }
    
    public void testStaticMethodReferenceDef() {
        assertEquals(1, exec("def l = new ArrayList(); l.add(2); l.add(1); l.sort(Integer::compare); return l.get(0);"));
    }

    public void testVirtualMethodReference() {
        assertEquals(2, exec("List l = new ArrayList(); l.add(1); l.add(1); return l.stream().mapToInt(Integer::intValue).sum();"));
    }
    
    public void testVirtualMethodReferenceDef() {
        assertEquals(2, exec("def l = new ArrayList(); l.add(1); l.add(1); return l.stream().mapToInt(Integer::intValue).sum();"));
    }

    public void testCtorMethodReference() {
        assertEquals(3.0D, 
            exec("List l = new ArrayList(); l.add(1.0); l.add(2.0); " + 
                 "DoubleStream doubleStream = l.stream().mapToDouble(Double::doubleValue);" + 
                 "DoubleSummaryStatistics stats = doubleStream.collect(DoubleSummaryStatistics::new, " +
                                                                      "DoubleSummaryStatistics::accept, " +
                                                                      "DoubleSummaryStatistics::combine); " + 
                 "return stats.getSum()"));
    }
    
    public void testCtorMethodReferenceDef() {
        assertEquals(3.0D, 
            exec("def l = new ArrayList(); l.add(1.0); l.add(2.0); " + 
                 "def doubleStream = l.stream().mapToDouble(Double::doubleValue);" + 
                 "def stats = doubleStream.collect(DoubleSummaryStatistics::new, " +
                                                  "DoubleSummaryStatistics::accept, " +
                                                  "DoubleSummaryStatistics::combine); " + 
                 "return stats.getSum()"));
    }

    public void testMethodMissing() {
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("List l = new ArrayList(); l.add(2); l.add(1); l.sort(Integer::bogus); return l.get(0);");
        });
        assertTrue(expected.getMessage().contains("Unknown reference"));
    }

    public void testNotFunctionalInterface() {
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("List l = new ArrayList(); l.add(2); l.add(1); l.add(Integer::bogus); return l.get(0);");
        });
        assertTrue(expected.getMessage().contains("Cannot convert function reference"));
    }

    public void testIncompatible() {
        expectScriptThrows(BootstrapMethodError.class, () -> {
            exec("List l = new ArrayList(); l.add(2); l.add(1); l.sort(String::startsWith); return l.get(0);");
        });
    }
}
