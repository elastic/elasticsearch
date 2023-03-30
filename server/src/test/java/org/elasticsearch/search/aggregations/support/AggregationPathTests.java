/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class AggregationPathTests extends ESTestCase {
    public void testInvalidPaths() {
        assertInvalidPath("[foo]", "brackets at the beginning of the token expression");
        assertInvalidPath("foo[bar", "open brackets without closing at the token expression");
        assertInvalidPath("foo[", "open bracket at the end of the token expression");
        assertInvalidPath("foo[]", "empty brackets in the token expression");
        assertInvalidPath("foo[bar]baz", "brackets not enclosing at the end of the token expression");
        assertInvalidPath(".foo", "dot separator at the beginning of the token expression");
        assertInvalidPath("foo.", "dot separator at the end of the token expression");
    }

    public void testValidPaths() {
        assertValidPath("foo[bar]._count", tokens().addKeyAndMetric("foo", "bar", "_count"));
        assertValidPath("foo>bar", tokens().add("foo").add("bar"));
        assertValidPath("foo.bar", tokens().addMetric("foo", "bar"));
        assertValidPath("foo[bar]", tokens().addKey("foo", "bar"));
        assertValidPath("foo[bar]>baz", tokens().addKey("foo", "bar").add("baz"));
        assertValidPath("foo[bar]>baz[qux]", tokens().addKey("foo", "bar").addKey("baz", "qux"));
        assertValidPath("foo[bar]>baz.qux", tokens().addKey("foo", "bar").addMetric("baz", "qux"));
        assertValidPath("foo.bar>baz.qux", tokens().add("foo.bar").addMetric("baz", "qux"));
        assertValidPath("foo.bar>baz[qux]", tokens().add("foo.bar").addKey("baz", "qux"));
    }

    private IllegalArgumentException assertInvalidPath(String path, String _unused) {
        return expectThrows(IllegalArgumentException.class, () -> AggregationPath.parse(path));
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
        private final List<AggregationPath.PathElement> tokens = new ArrayList<>();

        Tokens add(String name) {
            tokens.add(new AggregationPath.PathElement(name, name, null, null));
            return this;
        }

        Tokens addKey(String name, String key) {
            tokens.add(new AggregationPath.PathElement(name + "[" + key + "]", name, key, null));
            return this;
        }

        Tokens addMetric(String name, String metric) {
            tokens.add(new AggregationPath.PathElement(name + "." + metric, name, null, metric));
            return this;
        }

        Tokens addKeyAndMetric(String name, String key, String metric) {
            tokens.add(new AggregationPath.PathElement(name + "[" + key + "]." + metric, name, key, metric));
            return this;
        }

        AggregationPath.PathElement[] toArray() {
            return tokens.toArray(new AggregationPath.PathElement[0]);
        }
    }
}
