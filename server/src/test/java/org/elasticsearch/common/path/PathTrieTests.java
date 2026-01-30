/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.path;

import org.elasticsearch.common.path.PathTrie.TrieMatchingMode;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class PathTrieTests extends ESTestCase {

    public static final UnaryOperator<String> NO_DECODER = UnaryOperator.identity();

    public void testPath() {
        PathTrie<String> trie = new PathTrie<>(NO_DECODER);
        trie.insert("/a/b/c", "walla");
        trie.insert("a/d/g", "kuku");
        trie.insert("x/b/c", "lala");
        trie.insert("a/x/*", "one");
        trie.insert("a/b/*", "two");
        trie.insert("*/*/x", "three");
        trie.insert("{index}/insert/{docId}", "bingo");

        assertThat(trie.retrieve("a/b/c"), equalTo("walla"));
        assertThat(trie.retrieve("a/d/g"), equalTo("kuku"));
        assertThat(trie.retrieve("x/b/c"), equalTo("lala"));
        assertThat(trie.retrieve("a/x/b"), equalTo("one"));
        assertThat(trie.retrieve("a/b/d"), equalTo("two"));

        assertThat(trie.retrieve("a/b"), nullValue());
        assertThat(trie.retrieve("a/b/c/d"), nullValue());
        assertThat(trie.retrieve("g/t/x"), equalTo("three"));

        Map<String, String> params = new HashMap<>();
        assertThat(trie.retrieve("index1/insert/12", params), equalTo("bingo"));
        assertThat(params, equalTo(Map.of("index", "index1", "docId", "12")));
    }

    public void testEmptyPath() {
        PathTrie<String> trie = new PathTrie<>(NO_DECODER);
        trie.insert("/", "walla");
        assertThat(trie.retrieve(""), equalTo("walla"));
    }

    public void testDifferentNamesOnDifferentPath() {
        PathTrie<String> trie = new PathTrie<>(NO_DECODER);
        trie.insert("/a/{type}", "test1");
        trie.insert("/b/{name}", "test2");

        Map<String, String> params = new HashMap<>();
        assertThat(trie.retrieve("/a/test", params), equalTo("test1"));
        assertThat(params, equalTo(Map.of("type", "test")));

        params.clear();
        assertThat(trie.retrieve("/b/testX", params), equalTo("test2"));
        assertThat(params, equalTo(Map.of("name", "testX")));
    }

    public void testSameNameOnDifferentPath() {
        PathTrie<String> trie = new PathTrie<>(NO_DECODER);
        trie.insert("/a/c/{name}", "test1");
        trie.insert("/b/{name}", "test2");

        Map<String, String> params = new HashMap<>();
        assertThat(trie.retrieve("/a/c/test", params), equalTo("test1"));
        assertThat(params, equalTo(Map.of("name", "test")));

        params.clear();
        assertThat(trie.retrieve("/b/testX", params), equalTo("test2"));
        assertThat(params, equalTo(Map.of("name", "testX")));
    }

    public void testPreferNonWildcardExecution() {
        PathTrie<String> trie = new PathTrie<>(NO_DECODER);
        trie.insert("{test}", "test1");
        trie.insert("b", "test2");
        trie.insert("{test}/a", "test3");
        trie.insert("b/a", "test4");
        trie.insert("{test}/{testB}", "test5");
        trie.insert("{test}/x/{testC}", "test6");

        Map<String, String> params = new HashMap<>();
        assertThat(trie.retrieve("/b", params), equalTo("test2"));
        assertThat(trie.retrieve("/b/a", params), equalTo("test4"));
        assertThat(trie.retrieve("/v/x", params), equalTo("test5"));
        assertThat(trie.retrieve("/v/x/c", params), equalTo("test6"));
    }

    // https://github.com/elastic/elasticsearch/pull/17916
    public void testWildcardMatchingModes() {
        PathTrie<String> trie = new PathTrie<>(NO_DECODER);
        trie.insert("{testA}", "test1");
        trie.insert("{testA}/{testB}", "test2");
        trie.insert("a/{testB}", "test3");
        trie.insert("{testA}/b", "test4");
        trie.insert("{testA}/b/c", "test5");
        trie.insert("a/{testB}/c", "test6");
        trie.insert("a/b/{testC}", "test7");
        trie.insert("{testA}/b/{testB}", "test8");
        trie.insert("x/{testB}/z", "test9");
        trie.insert("{testA}/{testB}/{testC}", "test10");

        Map<String, String> params = new HashMap<>();

        assertThat(trie.retrieve("/a", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
        assertThat(trie.retrieve("/a", params, TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED), equalTo("test1"));
        assertThat(trie.retrieve("/a", params, TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED), equalTo("test1"));
        assertThat(trie.retrieve("/a", params, TrieMatchingMode.WILDCARD_NODES_ALLOWED), equalTo("test1"));
        assertThat(trie.retrieveAll("/a", () -> params).toList(), contains(null, "test1", "test1", "test1"));

        assertThat(trie.retrieve("/a/b", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
        assertThat(trie.retrieve("/a/b", params, TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED), equalTo("test4"));
        assertThat(trie.retrieve("/a/b", params, TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED), equalTo("test3"));
        assertThat(trie.retrieve("/a/b", params, TrieMatchingMode.WILDCARD_NODES_ALLOWED), equalTo("test3"));
        assertThat(trie.retrieveAll("/a/b", () -> params).toList(), contains(null, "test4", "test3", "test3"));

        assertThat(trie.retrieve("/a/b/c", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
        assertThat(trie.retrieve("/a/b/c", params, TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED), equalTo("test5"));
        assertThat(trie.retrieve("/a/b/c", params, TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED), equalTo("test7"));
        assertThat(trie.retrieve("/a/b/c", params, TrieMatchingMode.WILDCARD_NODES_ALLOWED), equalTo("test7"));
        assertThat(trie.retrieveAll("/a/b/c", () -> params).toList(), contains(null, "test5", "test7", "test7"));

        assertThat(trie.retrieve("/x/y/z", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
        assertThat(trie.retrieve("/x/y/z", params, TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED), nullValue());
        assertThat(trie.retrieve("/x/y/z", params, TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED), nullValue());
        assertThat(trie.retrieve("/x/y/z", params, TrieMatchingMode.WILDCARD_NODES_ALLOWED), equalTo("test9"));
        assertThat(trie.retrieveAll("/x/y/z", () -> params).toList(), contains(null, null, null, "test9"));

        assertThat(trie.retrieve("/d/e/f", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
        assertThat(trie.retrieve("/d/e/f", params, TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED), nullValue());
        assertThat(trie.retrieve("/d/e/f", params, TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED), nullValue());
        assertThat(trie.retrieve("/d/e/f", params, TrieMatchingMode.WILDCARD_NODES_ALLOWED), equalTo("test10"));
        assertThat(trie.retrieveAll("/d/e/f", () -> params).toList(), contains(null, null, null, "test10"));
    }

    // https://github.com/elastic/elasticsearch/pull/17916
    public void testExplicitMatchingMode() {
        PathTrie<String> trie = new PathTrie<>(NO_DECODER);
        trie.insert("{testA}", "test1");
        trie.insert("a", "test2");
        trie.insert("{testA}/{testB}", "test3");
        trie.insert("a/{testB}", "test4");
        trie.insert("{testA}/b", "test5");
        trie.insert("a/b", "test6");
        trie.insert("{testA}/b/{testB}", "test7");
        trie.insert("x/{testA}/z", "test8");
        trie.insert("{testA}/{testB}/{testC}", "test9");
        trie.insert("a/b/c", "test10");

        Map<String, String> params = new HashMap<>();

        assertThat(trie.retrieve("/a", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), equalTo("test2"));
        assertThat(trie.retrieve("/x", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
        assertThat(trie.retrieve("/a/b", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), equalTo("test6"));
        assertThat(trie.retrieve("/a/x", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
        assertThat(trie.retrieve("/a/b/c", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), equalTo("test10"));
        assertThat(trie.retrieve("/x/y/z", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
    }

    public void testSamePathConcreteResolution() {
        PathTrie<String> trie = new PathTrie<>(NO_DECODER);
        trie.insert("{x}/{y}/{z}", "test1");
        trie.insert("{x}/_y/{k}", "test2");

        Map<String, String> params = new HashMap<>();
        assertThat(trie.retrieve("/a/b/c", params), equalTo("test1"));
        assertThat(params, equalTo(Map.of("x", "a", "y", "b", "z", "c")));

        params.clear();
        assertThat(trie.retrieve("/a/_y/c", params), equalTo("test2"));
        assertThat(params, equalTo(Map.of("x", "a", "k", "c")));
    }

    public void testNamedWildcardAndLookupWithWildcard() {
        PathTrie<String> trie = new PathTrie<>(NO_DECODER);
        trie.insert("x/{test}", "test1");
        trie.insert("{test}/a", "test2");
        trie.insert("/{test}", "test3");
        trie.insert("/{test}/_endpoint", "test4");
        trie.insert("/*/{test}/_endpoint", "test5");

        Map<String, String> params = new HashMap<>();
        assertThat(trie.retrieve("/x/*", params), equalTo("test1"));
        assertThat(params, equalTo(Map.of("test", "*")));

        params.clear();
        assertThat(trie.retrieve("/b/a", params), equalTo("test2"));
        assertThat(params, equalTo(Map.of("test", "b")));

        params.clear();
        assertThat(trie.retrieve("/*", params), equalTo("test3"));
        assertThat(params, equalTo(Map.of("test", "*")));

        params.clear();
        assertThat(trie.retrieve("/*/_endpoint", params), equalTo("test4"));
        assertThat(params, equalTo(Map.of("test", "*")));

        params.clear();
        assertThat(trie.retrieve("a/*/_endpoint", params), equalTo("test5"));
        assertThat(params, equalTo(Map.of("test", "*")));
    }

    // https://github.com/elastic/elasticsearch/issues/14177
    // https://github.com/elastic/elasticsearch/issues/13665
    public void testEscapedSlashWithinUrl() {
        PathTrie<String> pathTrie = new PathTrie<>(RestUtils.REST_DECODER);
        pathTrie.insert("/{index}/{type}/{id}", "test");
        HashMap<String, String> params = new HashMap<>();

        assertThat(pathTrie.retrieve("/index/type/a%2Fe", params), equalTo("test"));
        assertThat(params, equalTo(Map.of("index", "index", "type", "type", "id", "a/e")));

        params.clear();
        assertThat(pathTrie.retrieve("/<logstash-{now%2Fd}>/type/id", params), equalTo("test"));
        assertThat(params, equalTo(Map.of("index", "<logstash-{now/d}>", "type", "type", "id", "id")));
    }
}
