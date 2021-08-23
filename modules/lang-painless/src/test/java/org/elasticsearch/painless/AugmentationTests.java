/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class AugmentationTests extends ScriptTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = super.scriptContexts();
        List<Whitelist> digestWhitelist = new ArrayList<>(PainlessPlugin.BASE_WHITELISTS);
        digestWhitelist.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script.ingest.txt"));
        contexts.put(DigestTestScript.CONTEXT, digestWhitelist);

        return contexts;
    }

    public abstract static class DigestTestScript {
        public static final String[] PARAMETERS = {};
        public abstract String execute();
        public interface Factory {
            DigestTestScript newInstance();
        }

        public static final ScriptContext<DigestTestScript.Factory> CONTEXT =
            new ScriptContext<>("test", DigestTestScript.Factory.class);
    }

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
        assertEquals(";empty;start;;test",
                exec("List l = new ArrayList(); l.add(''); l.add('empty'); l.add('start'); l.add(''); l.add('test'); l.join(';')"));
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

    public String execDigest(String script) {
        return scriptEngine.compile(
            "digest_test",
            script,
            DigestTestScript.CONTEXT, Collections.emptyMap()
        ).newInstance().execute();
    }

    public void testSha1() {
        assertEquals("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33", execDigest("'foo'.sha1()"));
        assertEquals("62cdb7020ff920e5aa642c3d4066950dd1f01f4d", execDigest("'bar'.sha1()"));
        assertEquals("5f5513f8822fdbe5145af33b64d8d970dcf95c6e", execDigest("'foobarbaz'.sha1()"));
    }

    public void testSha256() {
        assertEquals("2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae", execDigest("'foo'.sha256()"));
        assertEquals("fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9", execDigest("'bar'.sha256()"));
        assertEquals("97df3588b5a3f24babc3851b372f0ba71a9dcdded43b14b9d06961bfc1707d9d", execDigest("'foobarbaz'.sha256()"));
    }

    public void testToEpochMilli() {
        assertEquals(0L, exec("ZonedDateTime.parse('1970-01-01T00:00:00Z').toEpochMilli()"));
        assertEquals(1602097376782L, exec("ZonedDateTime.parse('2020-10-07T19:02:56.782Z').toEpochMilli()"));
    }

    public void testAugmentedField() {
        assertEquals(Integer.MAX_VALUE, exec("org.elasticsearch.painless.FeatureTestObject.MAX_VALUE"));
        assertEquals("test_string", exec("Integer.STRINGS[0]"));
        assertEquals("test_value", exec("Map.STRINGS['test_key']"));
        assertTrue((boolean)exec("Integer.STRINGS[0].substring(0, 4) == Map.STRINGS['test_key'].substring(0, 4)"));
    }
}
