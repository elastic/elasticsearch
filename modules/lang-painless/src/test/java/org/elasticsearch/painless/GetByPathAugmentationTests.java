/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
    private final Map<String, String> k001Obj = new HashMap<>();
    private final String k001MapStr = "['" + k001Key + "': '" + k001Value + "']";
    private final String mapMapList = "['k0': ['k01': [['k010': 'a'], " + k001MapStr + "]], 'k1': ['q']]";

    private final String l2m2l1Index0 = "ll0";
    private final String l2m2l1Index1 = "ll1";
    private final List<String> l2m2l1Obj = new ArrayList<>();
    private final String l2m2l1Str = "['" + l2m2l1Index0 + "', '" + l2m2l1Index1 + "']";
    private final String listMapListList = "[['m0':'v0'],['m1':'v1'],['m2':['l0','l1', " + l2m2l1Str + "]]]";

    private final String mapList = "['key0': ['a', 'b'], 'key1': ['c', 'd']]";
    private final String mapMap = "['a': ['b': 'c']]";

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

    private String numberFormat(String unparsable, String path, int i) {
        String format = "Could not parse [%s] as a int index into list at path [%s] and index [%d]";
        return String.format(Locale.ROOT, format, unparsable, path, i);
    }

    private String missingValue(String path) {
        return String.format(Locale.ROOT, "Could not find value at path [%s]", path);
    }

    private void assertPathValue(String collection, String key, Object value) {
        assertEquals(value, exec(toScript(collection, key)));
    }

    private void assertPathDefaultValue(String collection, String key, Object value, String defaultValue) {
        assertEquals(value, exec(toScript(collection, key, defaultValue)));
    }

    private IllegalArgumentException assertPathError(String collection, String key, String message) {
        return assertPathError(toScript(collection, key), message);
    }

    private IllegalArgumentException assertPathError(String collection, String key, String defaultValue, String message) {
        return assertPathError(toScript(collection, key, defaultValue), message);
    }

    private IllegalArgumentException assertPathError(String script, String message) {
        IllegalArgumentException illegal = expectScriptThrows(IllegalArgumentException.class, () -> exec(script));
        assertEquals(message, illegal.getMessage());
        return illegal;
    }

    public void testOneLevelMap() {
        assertPathValue("['k0':'v0']", "k0", "v0");
    }

    public void testOneLevelList() {
        assertPathValue("['a','b','c','d']", "2", "c");
    }

    public void testTwoLevelMapList() {
        assertPathValue("['key0': ['a', 'b'], 'key1': ['c', 'd']]", "key1.0", "c");
    }

    public void testMapDiffSizeList() {
        assertPathValue("['k0': ['a','b','c','d'], 'k1': ['q']]", "k0.3", "d");
    }

    public void testBiMapList() {
        assertPathValue(mapMapList, "k0.k01.1.k011", k001Value);
    }

    public void testBiMapListObject() {
        assertPathValue(mapMapList, "k0.k01.1", k001Obj);
    }

    public void testListMap() {
        assertPathValue("[['key0': 'value0'], ['key1': 'value1']]", "1.key1", "value1");
    }

    public void testTriList() {
        assertPathValue("[['a','b'],['c','d'],[['e','f'],['g','h']]]", "2.1.1", "h");
    }

    public void testMapBiListObject() {
        assertPathValue(listMapListList, "2.m2.2", l2m2l1Obj);
    }

    public void testMapBiList() {
        assertPathValue(listMapListList, "2.m2.2.1", l2m2l1Index1);
    }

    public void testGetCollection() {
        List<String> k1List = new ArrayList<>();
        k1List.add("c");
        k1List.add("d");
        assertPathValue("['key0': ['a', 'b'], 'key1': ['c', 'd']]", "key1", k1List);
    }

    public void testMapListDefaultOneLevel() {
        assertPathDefaultValue(mapList, "key2", "x", "'x'");
    }

    public void testMapListDefaultTwoLevel() {
        assertPathDefaultValue(mapList, "key1.1", "d", "'x'");
    }

    public void testBiMapListDefault() {
        assertPathDefaultValue(mapMapList, "k0.k01.1.k012", "foo", "'foo'");
    }

    public void testBiMapListDefaultExists() {
        assertPathDefaultValue(mapMapList, "k0.k01.1.k011", "b", "'foo'");
    }

    public void testBiMapListDefaultObjectExists() {
        assertPathDefaultValue(mapMapList, "k0.k01.1", k001Obj, "'foo'");
    }

    public void testBiMapListDefaultObject() {
        assertPathDefaultValue(mapMapList, "k0.k01.9", k001Obj, k001MapStr);
    }

    public void testListMapBiListDefaultExists() {
        assertPathDefaultValue(listMapListList, "2.m2.2", l2m2l1Obj, "'foo'");
    }

    public void testListMapBiListDefaultObject() {
        assertPathDefaultValue(listMapListList, "2.m2.9", l2m2l1Obj, l2m2l1Str);
    }

    public void testBiListBadIndex() {
        String path = "1.k0";
        IllegalArgumentException err = assertPathError("[['a','b'],['c','d']]", path, numberFormat("k0", path, 1));
        assertEquals(err.getCause().getClass(), NumberFormatException.class);
    }

    public void testBiMapListMissingLast() {
        String path = "k0.k01.1.k012";
        assertPathError(mapMapList, path, missingValue(path));
    }

    public void testBiMapListBadIndex() {
        String path = "k0.k01.k012";
        IllegalArgumentException err = assertPathError(mapMapList, path, numberFormat("k012", path, 2));
        assertEquals(err.getCause().getClass(), NumberFormatException.class);
    }

    public void testListMapBiListMissingObject() {
        String path = "2.m2.12";
        assertPathError(listMapListList, path, missingValue(path));
    }

    public void testListMapBiListBadIndexAtObject() {
        String path = "2.m2.a8";
        IllegalArgumentException err = assertPathError(listMapListList, path, numberFormat("a8", path, 2));
        assertEquals(err.getCause().getClass(), NumberFormatException.class);
    }

    public void testNonContainer() {
        assertPathError(mapMap, "a.b.c", "Non-container [java.lang.String] at [c], index [2] in path [a.b.c]");
    }

    public void testMissingPath() {
        assertPathError(mapMap, "", "Missing path");
    }

    public void testDoubleDot() {
        assertPathError(mapMap, "a..b", "Extra '.' in path [a..b] at index [1]");
    }

    public void testTrailingDot() {
        assertPathError(mapMap, "a.b.", "Trailing '.' in path [a.b.]");
    }

    public void testBiListDefaultBadIndex() {
        String path = "1.k0";
        IllegalArgumentException err = assertPathError("[['a','b'],['c','d']]", path, "'foo'", numberFormat("k0", path, 1));
        assertEquals(err.getCause().getClass(), NumberFormatException.class);
    }

    public void testBiMapListDefaultBadIndex() {
        String path = "k0.k01.k012";
        IllegalArgumentException err = assertPathError(mapMapList, path, "'foo'", numberFormat("k012", path, 2));
        assertEquals(err.getCause().getClass(), NumberFormatException.class);
    }

    public void testListMapBiListObjectDefaultBadIndex() {
        String path = "2.m2.a8";
        IllegalArgumentException err = assertPathError(listMapListList, path, "'foo'", numberFormat("a8", path, 2));
        assertEquals(err.getCause().getClass(), NumberFormatException.class);
    }

    public void testNonContainerDefaultBadIndex() {
        assertPathError(mapMap, "a.b.c", "'foo'", "Non-container [java.lang.String] at [c], index [2] in path [a.b.c]");
    }

    public void testDoubleDotDefault() {
        assertPathError(mapMap, "a..b", "'foo'", "Extra '.' in path [a..b] at index [1]");
    }

    public void testTrailingDotDefault() {
        assertPathError(mapMap, "a.b.", "'foo'", "Trailing '.' in path [a.b.]");
    }
}
