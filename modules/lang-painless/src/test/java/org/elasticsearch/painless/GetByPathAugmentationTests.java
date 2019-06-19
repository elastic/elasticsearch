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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class GetByPathAugmentationTests extends ScriptTestCase {

    private final String k001Key = "k011";
    private final String k001Value = "b";
    private final Map<String,String> k001Obj = new HashMap<>();
    private final String k001MapStr = "['" + k001Key + "': '" + k001Value + "']";
    private final String mapMapList = "['k0': ['k01': [['k010': 'a'], " + k001MapStr + "]], 'k1': ['q']]";

    private final String l2m2l1Index0 = "ll0";
    private final String l2m2l1Index1 = "ll1";
    private final List<String> l2m2l1Obj = new ArrayList<>();
    private final String l2m2l1Str = "['" + l2m2l1Index0 + "', '" + l2m2l1Index1 + "']";
    private final String listMapListList = "[['m0':'v0'],['m1':'v1'],['m2':['l0','l1', " + l2m2l1Str + "]]]";

    public GetByPathAugmentationTests() {
        l2m2l1Obj.add(l2m2l1Index0);
        l2m2l1Obj.add(l2m2l1Index1);
        k001Obj.put(k001Key, k001Value);
    }

    private String toScript(String collection, String key) {
        return String.format(Locale.ROOT, "return %s.getByPath('%s')", collection, key);
    }

    private String toScript(String collection, String key, String defaultValue) {
        return String.format(Locale.ROOT, "return %s.getByPath('%s', %s)", collection, key, defaultValue);
    }

    private IllegalArgumentException numberFormat(String unparsable, String path, int i) {
        String format = "Could not parse [%s] as a int index into list at path [%s] and index [%d]";
        return new IllegalArgumentException(String.format(Locale.ROOT, format, unparsable, path, i,
            new NumberFormatException()));
    }

    private IllegalArgumentException missingValue(String path) {
        String format = "Could not find value at path [%s]";
        return new IllegalArgumentException(String.format(Locale.ROOT, format, path));
    }


    private void assertPathValue(String collection, String key, Object value) {
        assertEquals(value, exec(toScript(collection, key)));
    }

    private void assertPathDefaultValue(String collection, String key, Object value, String defaultValue) {
        assertEquals(value, exec(toScript(collection, key, defaultValue)));
    }

    private void assertPathError(String collection, String key, IllegalArgumentException error) {
        assertPathError(toScript(collection, key), error);
    }

    private void assertPathError(String collection, String key, String defaultValue, IllegalArgumentException error) {
        assertPathError(toScript(collection, key, defaultValue), error);
    }

    private void assertPathError(String script, IllegalArgumentException error) {
        IllegalArgumentException illegal = expectScriptThrows(
            IllegalArgumentException.class,
            () -> exec(script)
        );
        assertEquals(error.getMessage(), illegal.getMessage());
        if (error.getCause() != null) {
            assertEquals(error.getCause().getClass(), illegal.getCause().getClass());
        }
    }

    public void testGetPathByValue() {
        assertPathValue("['k0':'v0']", "k0", "v0");
        assertPathValue("['a','b','c','d']", "2", "c");
        assertPathValue("['key0': ['a', 'b'], 'key1': ['c', 'd']]", "key1.0", "c");

        List<String> k1List = new ArrayList<>();
        k1List.add("c");
        k1List.add("d");
        assertPathValue("['key0': ['a', 'b'], 'key1': ['c', 'd']]", "key1", k1List);
        assertPathValue("['k0': ['a','b','c','d'], 'k1': ['q']]", "k0.3", "d");

        assertPathValue(mapMapList, "k0.k01.1", k001Obj);
        assertPathValue(mapMapList, "k0.k01.1.k011", k001Value);


        assertPathValue("[['key0': 'value0'], ['key1': 'value1']]", "1.key1", "value1");
        assertPathValue("[['a','b'],['c','d'],[['e','f'],['g','h']]]", "2.1.1", "h");

        assertPathValue(listMapListList, "2.m2.2", l2m2l1Obj);
        assertPathValue(listMapListList, "2.m2.2.1", l2m2l1Index1);
    }

    public void testGetPathByValueDefaultValue() {
        String mapList = "['key0': ['a', 'b'], 'key1': ['c', 'd']]";
        assertPathDefaultValue(mapList, "key2", "x", "'x'");
        assertPathDefaultValue(mapList, "key1.1", "d", "'x'");
        assertPathDefaultValue(mapMapList, "k0.k01.1.k012", "foo", "'foo'");
        assertPathDefaultValue(mapMapList, "k0.k01.1.k011", "b", "'foo'");
        assertPathDefaultValue(mapMapList, "k0.k01.1", k001Obj, "'foo'");
        assertPathDefaultValue(mapMapList, "k0.k01.9", k001Obj, k001MapStr);
        assertPathDefaultValue(listMapListList, "2.m2.2", l2m2l1Obj, "'foo'");
        assertPathDefaultValue(listMapListList, "2.m2.9", l2m2l1Obj, l2m2l1Str);
    }

    public void testGetPathByValueError() {
        String path = "1.k0";
        assertPathError("[['a','b'],['c','d']]", path, numberFormat("k0", path, 1));

        path = "k0.k01.1.k012";
        assertPathError(mapMapList, path, missingValue(path));

        path = "k0.k01.k012";
        assertPathError(mapMapList, path, numberFormat("k012", path, 2));

        path = "2.m2.12";
        assertPathError(listMapListList, path, missingValue(path));

        path = "2.m2.a8";
        assertPathError(listMapListList, path, numberFormat("a8", path, 2));

        String container = "['a': ['b': 'c']]";
        assertPathError(container, "a.b.c",
            new IllegalArgumentException("Non-container [java.lang.String] at [c], index [2] in path [a.b.c]"));

        assertPathError(container, "",
            new IllegalArgumentException("Missing path"));

        assertPathError(container, "a..b",
            new IllegalArgumentException("Extra '.' in path [a..b] at index [1]"));

        assertPathError(container, "a.b.",
            new IllegalArgumentException("Trailing '.' in path [a.b.]"));
    }

    public void testGetByValueDefaultError() {
        String path = "1.k0";
        assertPathError("[['a','b'],['c','d']]", path, "'foo'", numberFormat("k0", path, 1));

        path = "k0.k01.k012";
        assertPathError(mapMapList, path, "'foo'", numberFormat("k012", path, 2));

        path = "2.m2.a8";
        assertPathError(listMapListList, path, "'foo'", numberFormat("a8", path, 2));

        String container = "['a': ['b': 'c']]";
        assertPathError(container, "a.b.c", "'foo'",
            new IllegalArgumentException("Non-container [java.lang.String] at [c], index [2] in path [a.b.c]"));

        assertPathError(container, "", "'foo'",
            new IllegalArgumentException("Missing path"));

        assertPathError(container, "a..b", "'foo'",
            new IllegalArgumentException("Extra '.' in path [a..b] at index [1]"));

        assertPathError(container, "a.b.", "'foo'",
            new IllegalArgumentException("Trailing '.' in path [a.b.]"));
    }
}
