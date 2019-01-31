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

package org.elasticsearch.common.path;

import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import org.elasticsearch.common.path.PathTrie.TrieMatchingMode;

public class PathTrieTests extends ESTestCase {

    public static final PathTrie.Decoder NO_DECODER = new PathTrie.Decoder() {
        @Override
        public String decode(String value) {
            return value;
        }
    };

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
        assertThat(params.size(), equalTo(2));
        assertThat(params.get("index"), equalTo("index1"));
        assertThat(params.get("docId"), equalTo("12"));
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
        assertThat(params.get("type"), equalTo("test"));

        params.clear();
        assertThat(trie.retrieve("/b/testX", params), equalTo("test2"));
        assertThat(params.get("name"), equalTo("testX"));
    }

    public void testSameNameOnDifferentPath() {
        PathTrie<String> trie = new PathTrie<>(NO_DECODER);
        trie.insert("/a/c/{name}", "test1");
        trie.insert("/b/{name}", "test2");

        Map<String, String> params = new HashMap<>();
        assertThat(trie.retrieve("/a/c/test", params), equalTo("test1"));
        assertThat(params.get("name"), equalTo("test"));

        params.clear();
        assertThat(trie.retrieve("/b/testX", params), equalTo("test2"));
        assertThat(params.get("name"), equalTo("testX"));
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
        trie.insert("a/{testA}", "test3");
        trie.insert("{testA}/b", "test4");
        trie.insert("{testA}/b/c", "test5");
        trie.insert("a/{testB}/c", "test6");
        trie.insert("a/b/{testC}", "test7");
        trie.insert("{testA}/b/{testB}", "test8");
        trie.insert("x/{testA}/z", "test9");
        trie.insert("{testA}/{testB}/{testC}", "test10");

        Map<String, String> params = new HashMap<>();

        assertThat(trie.retrieve("/a", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
        assertThat(trie.retrieve("/a", params, TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED), equalTo("test1"));
        assertThat(trie.retrieve("/a", params, TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED), equalTo("test1"));
        assertThat(trie.retrieve("/a", params, TrieMatchingMode.WILDCARD_NODES_ALLOWED), equalTo("test1"));
        Iterator<String> allPaths = trie.retrieveAll("/a", () -> params);
        assertThat(allPaths.next(), equalTo(null));
        assertThat(allPaths.next(), equalTo("test1"));
        assertThat(allPaths.next(), equalTo("test1"));
        assertThat(allPaths.next(), equalTo("test1"));
        assertFalse(allPaths.hasNext());

        assertThat(trie.retrieve("/a/b", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
        assertThat(trie.retrieve("/a/b", params, TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED), equalTo("test4"));
        assertThat(trie.retrieve("/a/b", params, TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED), equalTo("test3"));
        assertThat(trie.retrieve("/a/b", params, TrieMatchingMode.WILDCARD_NODES_ALLOWED), equalTo("test3"));
        allPaths = trie.retrieveAll("/a/b", () -> params);
        assertThat(allPaths.next(), equalTo(null));
        assertThat(allPaths.next(), equalTo("test4"));
        assertThat(allPaths.next(), equalTo("test3"));
        assertThat(allPaths.next(), equalTo("test3"));
        assertFalse(allPaths.hasNext());

        assertThat(trie.retrieve("/a/b/c", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
        assertThat(trie.retrieve("/a/b/c", params, TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED), equalTo("test5"));
        assertThat(trie.retrieve("/a/b/c", params, TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED), equalTo("test7"));
        assertThat(trie.retrieve("/a/b/c", params, TrieMatchingMode.WILDCARD_NODES_ALLOWED), equalTo("test7"));
        allPaths = trie.retrieveAll("/a/b/c", () -> params);
        assertThat(allPaths.next(), equalTo(null));
        assertThat(allPaths.next(), equalTo("test5"));
        assertThat(allPaths.next(), equalTo("test7"));
        assertThat(allPaths.next(), equalTo("test7"));
        assertFalse(allPaths.hasNext());

        assertThat(trie.retrieve("/x/y/z", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
        assertThat(trie.retrieve("/x/y/z", params, TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED), nullValue());
        assertThat(trie.retrieve("/x/y/z", params, TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED), nullValue());
        assertThat(trie.retrieve("/x/y/z", params, TrieMatchingMode.WILDCARD_NODES_ALLOWED), equalTo("test9"));
        allPaths = trie.retrieveAll("/x/y/z", () -> params);
        assertThat(allPaths.next(), equalTo(null));
        assertThat(allPaths.next(), equalTo(null));
        assertThat(allPaths.next(), equalTo(null));
        assertThat(allPaths.next(), equalTo("test9"));
        assertFalse(allPaths.hasNext());

        assertThat(trie.retrieve("/d/e/f", params, TrieMatchingMode.EXPLICIT_NODES_ONLY), nullValue());
        assertThat(trie.retrieve("/d/e/f", params, TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED), nullValue());
        assertThat(trie.retrieve("/d/e/f", params, TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED), nullValue());
        assertThat(trie.retrieve("/d/e/f", params, TrieMatchingMode.WILDCARD_NODES_ALLOWED), equalTo("test10"));
        allPaths = trie.retrieveAll("/d/e/f", () -> params);
        assertThat(allPaths.next(), equalTo(null));
        assertThat(allPaths.next(), equalTo(null));
        assertThat(allPaths.next(), equalTo(null));
        assertThat(allPaths.next(), equalTo("test10"));
        assertFalse(allPaths.hasNext());
    }

    // https://github.com/elastic/elasticsearch/pull/17916
    public void testExplicitMatchingMode() {
        PathTrie<String> trie = new PathTrie<>(NO_DECODER);
        trie.insert("{testA}", "test1");
        trie.insert("a", "test2");
        trie.insert("{testA}/{testB}", "test3");
        trie.insert("a/{testB}", "test4");
        trie.insert("{testB}/b", "test5");
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
        assertThat(params.get("x"), equalTo("a"));
        assertThat(params.get("y"), equalTo("b"));
        assertThat(params.get("z"), equalTo("c"));
        params.clear();
        assertThat(trie.retrieve("/a/_y/c", params), equalTo("test2"));
        assertThat(params.get("x"), equalTo("a"));
        assertThat(params.get("k"), equalTo("c"));
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
        assertThat(params.get("test"), equalTo("*"));

        params = new HashMap<>();
        assertThat(trie.retrieve("/b/a", params), equalTo("test2"));
        assertThat(params.get("test"), equalTo("b"));

        params = new HashMap<>();
        assertThat(trie.retrieve("/*", params), equalTo("test3"));
        assertThat(params.get("test"), equalTo("*"));

        params = new HashMap<>();
        assertThat(trie.retrieve("/*/_endpoint", params), equalTo("test4"));
        assertThat(params.get("test"), equalTo("*"));

        params = new HashMap<>();
        assertThat(trie.retrieve("a/*/_endpoint", params), equalTo("test5"));
        assertThat(params.get("test"), equalTo("*"));
    }

    //https://github.com/elastic/elasticsearch/issues/14177
    //https://github.com/elastic/elasticsearch/issues/13665
    public void testEscapedSlashWithinUrl() {
        PathTrie<String> pathTrie = new PathTrie<>(RestUtils.REST_DECODER);
        pathTrie.insert("/{index}/{type}/{id}", "test");
        HashMap<String, String> params = new HashMap<>();
        assertThat(pathTrie.retrieve("/index/type/a%2Fe", params), equalTo("test"));
        assertThat(params.get("index"), equalTo("index"));
        assertThat(params.get("type"), equalTo("type"));
        assertThat(params.get("id"), equalTo("a/e"));
        params.clear();
        assertThat(pathTrie.retrieve("/<logstash-{now%2Fd}>/type/id", params), equalTo("test"));
        assertThat(params.get("index"), equalTo("<logstash-{now/d}>"));
        assertThat(params.get("type"), equalTo("type"));
        assertThat(params.get("id"), equalTo("id"));
    }
}
