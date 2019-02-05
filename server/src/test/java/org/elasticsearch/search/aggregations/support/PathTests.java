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

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class PathTests extends ESTestCase {
    public void testInvalidPaths() throws Exception {
        assertInvalidPath("[foo]", "brackets at the beginning of the token expression");
        assertInvalidPath("foo[bar", "open brackets without closing at the token expression");
        assertInvalidPath("foo[", "open bracket at the end of the token expression");
        assertInvalidPath("foo[]", "empty brackets in the token expression");
        assertInvalidPath("foo[bar]baz", "brackets not enclosing at the end of the token expression");
        assertInvalidPath(".foo", "dot separator at the beginning of the token expression");
        assertInvalidPath("foo.", "dot separator at the end of the token expression");
    }

    public void testValidPaths() throws Exception {
        assertValidPath("foo>bar", tokens().add("foo").add("bar"));
        assertValidPath("foo.bar", tokens().add("foo", "bar"));
        assertValidPath("foo[bar]", tokens().add("foo", "bar"));
        assertValidPath("foo[bar]>baz", tokens().add("foo", "bar").add("baz"));
        assertValidPath("foo[bar]>baz[qux]", tokens().add("foo", "bar").add("baz", "qux"));
        assertValidPath("foo[bar]>baz.qux", tokens().add("foo", "bar").add("baz", "qux"));
        assertValidPath("foo.bar>baz.qux", tokens().add("foo.bar").add("baz", "qux"));
        assertValidPath("foo.bar>baz[qux]", tokens().add("foo.bar").add("baz", "qux"));
    }

    private void assertInvalidPath(String path, String reason) {
        try {
            AggregationPath.parse(path);
            fail("Expected parsing path [" + path + "] to fail - " + reason);
        } catch (AggregationExecutionException aee) {
            // expected
        }
    }

    private void assertValidPath(String path, Tokens tokenz) {
        AggregationPath.PathElement[] tokens = tokenz.toArray();
        AggregationPath p = AggregationPath.parse(path);
        assertThat(p.getPathElements().size(), equalTo(tokens.length));
        for (int i = 0; i < p.getPathElements().size(); i++) {
            AggregationPath.PathElement t1 = p.getPathElements().get(i);
            AggregationPath.PathElement t2 = tokens[i];
            assertThat(t1, equalTo(t2));
        }
    }

    private static Tokens tokens() {
        return new Tokens();
    }

    private static class Tokens {
        private List<AggregationPath.PathElement> tokens = new ArrayList<>();

        Tokens add(String name) {
            tokens.add(new AggregationPath.PathElement(name, name, null));
            return this;
        }

        Tokens add(String name, String key) {
            if (randomBoolean()) {
                tokens.add(new AggregationPath.PathElement(name + "." + key, name, key));
            } else {
                tokens.add(new AggregationPath.PathElement(name + "[" + key + "]", name, key));
            }
            return this;
        }

        AggregationPath.PathElement[] toArray() {
            return tokens.toArray(new AggregationPath.PathElement[tokens.size()]);
        }
    }
}
