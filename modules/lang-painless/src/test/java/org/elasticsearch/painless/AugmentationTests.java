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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

public class AugmentationTests extends ScriptTestCase {

    public void testStatic() {
        assertEquals(1, exec("ArrayList l = new ArrayList(); l.add(1); return l.getLength();"));
        assertEquals(1, exec("ArrayList l = new ArrayList(); l.add(1); return l.length;"));
    }

    public void testSubclass() {
        assertEquals(1, exec("List l = new ArrayList(); l.add(1); return l.getLength();"));
        assertEquals(1, exec("List l = new ArrayList(); l.add(1); return l.length;"));
    }

    public void testDef() {
        assertEquals(1, exec("def l = new ArrayList(); l.add(1); return l.getLength();"));
        assertEquals(1, exec("def l = new ArrayList(); l.add(1); return l.length;"));
    }

    public void testCapturingReference() {
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
        assertEquals(true,
                exec("List l = new ArrayList(); l.add(1); l.any(x -> x == 1)"));
    }

    public void testIterable_AsCollection() {
        assertEquals(true,
                exec("List l = new ArrayList(); return l.asCollection() === l"));
    }

    public void testIterable_AsList() {
        assertEquals(true,
                exec("List l = new ArrayList(); return l.asList() === l"));
        assertEquals(5,
                exec("Set l = new HashSet(); l.add(5); return l.asList()[0]"));
    }

    public void testIterable_Each() {
        assertEquals(1,
                exec("List l = new ArrayList(); l.add(1); List l2 = new ArrayList(); l.each(l2::add); return l2.size()"));
    }

    public void testIterable_EachWithIndex() {
        assertEquals(0,
                exec("List l = new ArrayList(); l.add(2); Map m = new HashMap(); l.eachWithIndex(m::put); return m.get(2)"));
    }

    public void testIterable_Every() {
        assertEquals(false, exec("List l = new ArrayList(); l.add(1); l.add(2); l.every(x -> x == 1)"));
    }

    public void testIterable_FindResults() {
        assertEquals(1,
                exec("List l = new ArrayList(); l.add(1); l.add(2); l.findResults(x -> x == 1 ? x : null).size()"));
    }

    public void testIterable_GroupBy() {
        assertEquals(2,
                exec("List l = new ArrayList(); l.add(1); l.add(-1); l.groupBy(x -> x < 0 ? 'negative' : 'positive').size()"));
    }

    public void testIterable_Join() {
        assertEquals("test,ing",
                exec("List l = new ArrayList(); l.add('test'); l.add('ing'); l.join(',')"));
    }

    public void testIterable_Sum() {
        assertEquals(3.0D, exec("def l = [1,2]; return l.sum()"));
        assertEquals(5.0D,
                exec("List l = new ArrayList(); l.add(1); l.add(2); l.sum(x -> x + 1)"));
    }

    public void testCollection_Collect() {
        assertEquals(Arrays.asList(2, 3),
                exec("List l = new ArrayList(); l.add(1); l.add(2); l.collect(x -> x + 1)"));
        assertEquals(asSet(2, 3),
                exec("List l = new ArrayList(); l.add(1); l.add(2); l.collect(new HashSet(), x -> x + 1)"));
    }

    public void testCollection_Find() {
        assertEquals(2,
                exec("List l = new ArrayList(); l.add(1); l.add(2); return l.find(x -> x == 2)"));
    }

    public void testCollection_FindAll() {
        assertEquals(Arrays.asList(2),
                exec("List l = new ArrayList(); l.add(1); l.add(2); return l.findAll(x -> x == 2)"));
    }

    public void testCollection_FindResult() {
        assertEquals("found",
                exec("List l = new ArrayList(); l.add(1); l.add(2); return l.findResult(x -> x > 1 ? 'found' : null)"));
        assertEquals("notfound",
                exec("List l = new ArrayList(); l.add(1); l.add(2); return l.findResult('notfound', x -> x > 10 ? 'found' : null)"));
    }

    public void testCollection_Split() {
        assertEquals(Arrays.asList(Arrays.asList(2), Arrays.asList(1)),
                exec("List l = new ArrayList(); l.add(1); l.add(2); return l.split(x -> x == 2)"));
    }

    public void testMap_Collect() {
        assertEquals(Arrays.asList("one1", "two2"),
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; m.collect((key,value) -> key + value)"));
        assertEquals(asSet("one1", "two2"),
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; m.collect(new HashSet(), (key,value) -> key + value)"));
    }

    public void testMap_Count() {
        assertEquals(1,
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; m.count((key,value) -> value == 2)"));
    }

    public void testMap_Each() {
        assertEquals(2,
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; Map m2 = new TreeMap(); m.each(m2::put); return m2.size()"));
    }

    public void testMap_Every() {
        assertEquals(false,
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; m.every((key,value) -> value == 2)"));
    }

    public void testMap_Find() {
        assertEquals("two",
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; return m.find((key,value) -> value == 2).key"));
    }

    public void testMap_FindAll() {
        assertEquals(Collections.singletonMap("two", 2),
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; return m.findAll((key,value) -> value == 2)"));
    }

    public void testMap_FindResult() {
        assertEquals("found",
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; return m.findResult((key,value) -> value == 2 ? 'found' : null)"));
        assertEquals("notfound",
                exec("Map m = new TreeMap(); m.one = 1; m.two = 2; " +
                     "return m.findResult('notfound', (key,value) -> value == 10 ? 'found' : null)"));
    }

    public void testMap_FindResults() {
        assertEquals(Arrays.asList("negative", "positive"),
                exec("Map m = new TreeMap(); m.a = -1; m.b = 1; " +
                     "return m.findResults((key,value) -> value < 0 ? 'negative' : 'positive')"));
    }

    public void testMap_GroupBy() {
        Map<String,Map<String,Integer>> expected = new HashMap<>();
        expected.put("negative", Collections.singletonMap("a", -1));
        expected.put("positive", Collections.singletonMap("b", 1));
        assertEquals(expected,
                exec("Map m = new TreeMap(); m.a = -1; m.b = 1; " +
                     "return m.groupBy((key,value) -> value < 0 ? 'negative' : 'positive')"));
    }

    public void testFeatureTest() {
        assertEquals(5, exec("org.elasticsearch.painless.FeatureTestObject ft = new org.elasticsearch.painless.FeatureTestObject();" +
            " ft.setX(3); ft.setY(2); return ft.getTotal()"));
        assertEquals(5, exec("def ft = new org.elasticsearch.painless.FeatureTestObject();" +
            " ft.setX(3); ft.setY(2); return ft.getTotal()"));
        assertEquals(8, exec("org.elasticsearch.painless.FeatureTestObject ft = new org.elasticsearch.painless.FeatureTestObject();" +
            " ft.setX(3); ft.setY(2); return ft.addToTotal(3)"));
        assertEquals(8, exec("def ft = new org.elasticsearch.painless.FeatureTestObject();" +
            " ft.setX(3); ft.setY(2); return ft.addToTotal(3)"));
    }

    private static class SplitCase {
        final String input;
        final String token;
        final int count;

        SplitCase(String input, String token, int count) {
            this.input = input;
            this.token = token;
            this.count = count;
        }
        SplitCase(String input, String token) {
            this(input, token, -1);
        }
    }
    public void testString_SplitOnToken() {
        SplitCase[] cases = new SplitCase[] {
            new SplitCase("", ""),
            new SplitCase("a,b,c", ","),
            new SplitCase("a,b,c", "|"),
            new SplitCase("a,,b,,c", ","),
            new SplitCase("a,,b,,c", ",", 1),
            new SplitCase("a,,b,,c", ",", 3),
            new SplitCase("a,,b,,c", ",", 300),
            new SplitCase("a,b,c", "a,b,c,d"),
            new SplitCase("aaaaaaa", "a"),
            new SplitCase("aaaaaaa", "a", 2),
            new SplitCase("1.1.1.1.111", "1"),
            new SplitCase("1.1.1.1.111", "."),
            new SplitCase("1\n1.1.\r\n1\r\n111", "\r\n"),
        };
        for (SplitCase split : cases) {
            assertArrayEquals(
                split.input.split(Pattern.quote(split.token), split.count),
                (String[])exec("return \""+split.input+"\".splitOnToken(\""+split.token+"\", "+split.count+");")
            );
        }
    }

    private static class GetByPathCase {
        final String collection;
        final String key;
        final Object value;
        final String defaultValue;
        final boolean isDefaultValue;
        final IllegalArgumentException illegalError;
        final NumberFormatException numberError;

        GetByPathCase(String collection, String key, Object value) {
            this(collection, key, value, null, false, null);
        }

        GetByPathCase(String collection, String key, Exception error) {
            this(collection, key, null, null, false, error);
        }

        GetByPathCase(String collection, String key, Object value, String defaultValue) {
            this(collection, key, value, defaultValue, true, null);
        }

        GetByPathCase(String collection, String key, String defaultValue, Exception error) {
            this(collection, key, null, defaultValue, true, error);
        }

        private GetByPathCase(String collection, String key, Object value, String defaultValue, boolean isDefaultValue, Exception error) {
            this.collection = collection;
            this.key = key;
            this.value = value;
            this.defaultValue = defaultValue;
            this.isDefaultValue = isDefaultValue;
            if (error == null) {
                this.numberError = null;
                this.illegalError = null;
            } else {
                if (error instanceof NumberFormatException) {
                    this.numberError = (NumberFormatException)error;
                    this.illegalError = null;
                } else if (error instanceof IllegalArgumentException) {
                    this.illegalError = (IllegalArgumentException)error;
                    this.numberError = null;
                } else {
                    this.numberError = null;
                    this.illegalError = null;
                }
            }
        }

        String Script() {
            String format = "return %s.getByPath('%s'%s)";
            String defaultValue = (this.isDefaultValue ? ", " + this.defaultValue : "");
            return String.format(Locale.ROOT, format, this.collection, this.key, defaultValue);
        }
    }

    private NumberFormatException numberFormat(String unparsable, String path, int index) {
        String format = "Could not parse [%s] as a int index into list at path [%s] and index [%d]";
        return new NumberFormatException(String.format(Locale.ROOT, format, unparsable, path, index));
    }

    private IllegalArgumentException illegalArg(String path) {
        String format = "Could not find value at path [%s]";
        return new IllegalArgumentException(String.format(Locale.ROOT, format, path));
    }

    public void testGetByPath() {
        Map<String,String> k001 = new HashMap<>();
        String k001Key = "k011";
        k001.put(k001Key, "b");

        List<String> ll2 = new ArrayList<>();
        ll2.add("ll0");
        String ll2Index1 = "ll1";
        ll2.add(ll2Index1);

        List<String> k1List = new ArrayList<>();
        k1List.add("c");
        k1List.add("d");

        String mapMapList = "['k0': ['k01': [['k010': 'a'], ['k011': 'b']]], 'k1': ['q']]";
        String listMapListList = "[['m0':'v0'],['m1':'v1'],['m2':['l0','l1', ['ll0', 'll1']]]]";

        GetByPathCase[] cases = new GetByPathCase[] {
          // basic
          new GetByPathCase("['k0':'v0']", "k0", "v0"),
          new GetByPathCase("['a','b','c','d']", "2", "c"),
          new GetByPathCase("[['a','b'],['c','d']]", "1.k0", "'c'", numberFormat("k0", "1.k0", 1)),
          new GetByPathCase("[['a','b'],['c','d']]", "1.k0", numberFormat("k0", "1.k0", 1)),
          new GetByPathCase(mapMapList, "k0.k01.1.k012", illegalArg("k0.k01.1.k012")),

          // nesting, map first
          new GetByPathCase("['key0': ['a', 'b'], 'key1': ['c', 'd']]", "key1.0", "c"),
          new GetByPathCase("['key0': ['a', 'b'], 'key1': ['c', 'd']]", "key1", k1List),
          new GetByPathCase("['key0': ['a', 'b'], 'key1': ['c', 'd']]", "key2", "x", "'x'"),
          new GetByPathCase("['k0': ['a','b','c','d'], 'k1': ['q']]", "k0.3", "d"),
          new GetByPathCase(mapMapList, "k0.k01.1", k001),
          new GetByPathCase(mapMapList, "k0.k01.1.k011", k001.get(k001Key)),
          new GetByPathCase(mapMapList, "k0.k01.1.k012", "foo", "'foo'"),
          new GetByPathCase(mapMapList, "k0.k01.1.k012", illegalArg("k0.k01.1.k012")),
          new GetByPathCase(mapMapList, "k0.k01.k012", numberFormat("k012", "k0.k01.k012", 2)),
          new GetByPathCase(mapMapList, "k0.k01.k012", "'q'", numberFormat("k012", "k0.k01.k012", 2)),

          // nesting, list first
          new GetByPathCase("[['key0': 'value0'], ['key1': 'value1']]", "1.key1", "value1"),
          new GetByPathCase("[['a','b'],['c','d'],[['e','f'],['g','h']]]", "2.1.1", "h"),
          new GetByPathCase(listMapListList, "2.m2.2.1", ll2Index1),
          new GetByPathCase(listMapListList, "2.m2.2", ll2),
          new GetByPathCase(listMapListList, "2.m2.12", illegalArg("2.m2.12")),
          new GetByPathCase(listMapListList, "2.m2.a8", numberFormat("a8", "2.m2.a8", 2)),
          new GetByPathCase(listMapListList, "2.m2.a8", "'r'", numberFormat("a8", "2.m2.a8", 2)),
        };
        for (GetByPathCase getCase: cases) {
            if (getCase.illegalError != null) {
                IllegalArgumentException illegal = expectScriptThrows(IllegalArgumentException.class, () -> exec(getCase.Script()));
                assertEquals(getCase.illegalError.getMessage(), illegal.getMessage());
            } else if (getCase.numberError != null) {
                NumberFormatException number = expectScriptThrows(NumberFormatException.class, () -> exec(getCase.Script()));
                assertEquals(getCase.numberError.getMessage(), number.getMessage());
            } else {
                assertEquals(getCase.value, exec(getCase.Script()));
            }
        }
    }
}
