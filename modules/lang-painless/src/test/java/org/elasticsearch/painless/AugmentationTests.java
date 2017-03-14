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

import org.apache.lucene.util.Constants;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AugmentationTests extends ScriptTestCase {
    
    public void testStatic() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, exec("ArrayList l = new ArrayList(); l.add(1); return l.getLength();"));
        assertEquals(1, exec("ArrayList l = new ArrayList(); l.add(1); return l.length;"));
    }
    
    public void testSubclass() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, exec("List l = new ArrayList(); l.add(1); return l.getLength();"));
        assertEquals(1, exec("List l = new ArrayList(); l.add(1); return l.length;"));
    }
    
    public void testDef() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, exec("def l = new ArrayList(); l.add(1); return l.getLength();"));
        assertEquals(1, exec("def l = new ArrayList(); l.add(1); return l.length;"));
    }
    
    public void testCapturingReference() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, exec("int foo(Supplier t) { return t.get() }" +
                             "ArrayList l = new ArrayList(); l.add(1);" +
                             "return foo(l::getLength);"));
        assertEquals(1, exec("int foo(Supplier t) { return t.get() }" +
                             "List l = new ArrayList(); l.add(1);" +
                             "return foo(l::getLength);"));
        assertEquals(1, exec("int foo(Supplier t) { return t.get() }" +
                             "def l = new ArrayList(); l.add(1);" +
                             "return foo(l::getLength);"));
    }
    
    public void testIterable_Any() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(true, 
                exec("List l = new ArrayList(); l.add(1); l.any(x -> x == 1)"));
    }
    
    public void testIterable_AsCollection() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(true, 
                exec("List l = new ArrayList(); return l.asCollection() === l"));
    }
    
    public void testIterable_AsList() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(true, 
                exec("List l = new ArrayList(); return l.asList() === l"));
        assertEquals(5, 
                exec("Set l = new HashSet(); l.add(5); return l.asList()[0]"));
    }
    
    public void testIterable_Each() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, 
                exec("List l = new ArrayList(); l.add(1); List l2 = new ArrayList(); l.each(l2::add); return l2.size()"));
    }
    
    public void testIterable_EachWithIndex() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(0, 
                exec("List l = new ArrayList(); l.add(2); Map m = new HashMap(); l.eachWithIndex(m::put); return m.get(2)"));
    }
    
    public void testIterable_Every() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(false, exec("List l = new ArrayList(); l.add(1); l.add(2); l.every(x -> x == 1)"));
    }
    
    public void testIterable_FindResults() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, 
                exec("List l = new ArrayList(); l.add(1); l.add(2); l.findResults(x -> x == 1 ? x : null).size()"));
    }
    
    public void testIterable_GroupBy() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, 
                exec("List l = new ArrayList(); l.add(1); l.add(-1); l.groupBy(x -> x < 0 ? 'negative' : 'positive').size()"));
    }
    
    public void testIterable_Join() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("test,ing", 
                exec("List l = new ArrayList(); l.add('test'); l.add('ing'); l.join(',')"));
    }
    
    public void testIterable_Sum() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(3.0D, exec("def l = [1,2]; return l.sum()"));
        assertEquals(5.0D, 
                exec("List l = new ArrayList(); l.add(1); l.add(2); l.sum(x -> x + 1)"));
    }
    
    public void testCollection_Collect() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(Arrays.asList(2, 3), 
                exec("List l = new ArrayList(); l.add(1); l.add(2); l.collect(x -> x + 1)"));
        assertEquals(asSet(2, 3), 
                exec("List l = new ArrayList(); l.add(1); l.add(2); l.collect(new HashSet(), x -> x + 1)"));
    }
    
    public void testCollection_Find() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, 
                exec("List l = new ArrayList(); l.add(1); l.add(2); return l.find(x -> x == 2)"));
    }
    
    public void testCollection_FindAll() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(Arrays.asList(2), 
                exec("List l = new ArrayList(); l.add(1); l.add(2); return l.findAll(x -> x == 2)"));
    }
    
    public void testCollection_FindResult() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("found", 
                exec("List l = new ArrayList(); l.add(1); l.add(2); return l.findResult(x -> x > 1 ? 'found' : null)"));
        assertEquals("notfound", 
                exec("List l = new ArrayList(); l.add(1); l.add(2); return l.findResult('notfound', x -> x > 10 ? 'found' : null)"));
    }
    
    public void testCollection_Split() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(Arrays.asList(Arrays.asList(2), Arrays.asList(1)), 
                exec("List l = new ArrayList(); l.add(1); l.add(2); return l.split(x -> x == 2)"));
    }
    
    public void testMap_Collect() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(Arrays.asList("one1", "two2"), 
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; m.collect((key,value) -> key + value)"));
        assertEquals(asSet("one1", "two2"), 
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; m.collect(new HashSet(), (key,value) -> key + value)"));
    }
    
    public void testMap_Count() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(1, 
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; m.count((key,value) -> value == 2)"));
    }
    
    public void testMap_Each() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(2, 
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; Map m2 = new TreeMap(); m.each(m2::put); return m2.size()"));
    }
    
    public void testMap_Every() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(false, 
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; m.every((key,value) -> value == 2)"));
    }
    
    public void testMap_Find() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("two", 
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; return m.find((key,value) -> value == 2).key"));
    }
    
    public void testMap_FindAll() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(Collections.singletonMap("two", 2), 
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; return m.findAll((key,value) -> value == 2)"));
    }
    
    public void testMap_FindResult() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals("found", 
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; return m.findResult((key,value) -> value == 2 ? 'found' : null)"));
        assertEquals("notfound", 
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; " + 
                     "return m.findResult('notfound', (key,value) -> value == 10 ? 'found' : null)"));
    }
    
    public void testMap_FindResults() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        assertEquals(Arrays.asList("negative", "positive"),
                exec("Map m = new TreeMap(); m.a = -1; m.b = 1; " + 
                     "return m.findResults((key,value) -> value < 0 ? 'negative' : 'positive')"));
    }
    
    public void testMap_GroupBy() {
        assumeFalse("JDK is JDK 9", Constants.JRE_IS_MINIMUM_JAVA9);
        Map<String,Map<String,Integer>> expected = new HashMap<>();
        expected.put("negative", Collections.singletonMap("a", -1));
        expected.put("positive", Collections.singletonMap("b", 1));
        assertEquals(expected,
                exec("Map m = new TreeMap(); m.a = -1; m.b = 1; " + 
                     "return m.groupBy((key,value) -> value < 0 ? 'negative' : 'positive')"));
    }
}
