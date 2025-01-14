/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SelectorResolver;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.action.support.IndexComponentSelector.ALL_APPLICABLE;
import static org.elasticsearch.action.support.IndexComponentSelector.DATA;
import static org.elasticsearch.action.support.IndexComponentSelector.FAILURES;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.Context;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class SelectorResolverTests extends ESTestCase {

    public void testResolveExpression() {
        // === Parsing and defaults
        // Allow selectors TRUE
        Context selectorsAllowed = getContext(getOptionsForSelectors());

        assertThat(resolve(selectorsAllowed, "testXXX"), equalTo(new ResolvedExpression("testXXX", DATA)));
        assertThat(resolve(selectorsAllowed, "testXXX::data"), equalTo(new ResolvedExpression("testXXX", DATA)));
        assertThat(resolve(selectorsAllowed, "testXXX::failures"), equalTo(new ResolvedExpression("testXXX", FAILURES)));
        assertThat(resolve(selectorsAllowed, "testXXX::*"), equalTo(new ResolvedExpression("testXXX", ALL_APPLICABLE)));

        // Disallow selectors (example: creating, modifying, or deleting indices/data streams/aliases).
        // Accepts standard expressions but throws when selectors are specified.
        Context noSelectors = getContext(getDisabledSelectorOptions());

        assertThat(resolve(noSelectors, "testXXX"), equalTo(new ResolvedExpression("testXXX")));
        expectThrows(IllegalArgumentException.class, () -> resolve(noSelectors, "testXXX::data"));
        expectThrows(IllegalArgumentException.class, () -> resolve(noSelectors, "testXXX::failures"));
        expectThrows(IllegalArgumentException.class, () -> resolve(noSelectors, "testXXX::*"));

        // === Errors
        // Only recognized components can be selected
        expectThrows(InvalidIndexNameException.class, () -> resolve(selectorsAllowed, "testXXX::custom"));
        // Spelling is important
        expectThrows(InvalidIndexNameException.class, () -> resolve(selectorsAllowed, "testXXX::failres"));
        // Only the match all wildcard is supported
        expectThrows(InvalidIndexNameException.class, () -> resolve(selectorsAllowed, "testXXX::d*ta"));
        // Only one selector separator is allowed per expression
        expectThrows(InvalidIndexNameException.class, () -> resolve(selectorsAllowed, "test::XXX::data"));

        // === Wildcards, Date Math, and edge cases
        // Wildcards are left as-is (handled in wildcard resolver)
        assertThat(resolve(selectorsAllowed, "*"), equalTo(new ResolvedExpression("*", DATA)));
        // Exclusions are left as-is (if no wildcards are present they are not treated as exclusions)
        assertThat(resolve(selectorsAllowed, "-testXXX"), equalTo(new ResolvedExpression("-testXXX", DATA)));
        // Exclusion syntax with selectors will have the selectors parsed
        assertThat(resolve(selectorsAllowed, "-testXXX::failures"), equalTo(new ResolvedExpression("-testXXX", FAILURES)));
        // Date math is left unprocessed (handled later in date math resolver)
        assertThat(resolve(selectorsAllowed, "<test-{now/d}>"), equalTo(new ResolvedExpression("<test-{now/d}>", DATA)));
        // Providing a selector requires adding after the date math brackets
        assertThat(resolve(selectorsAllowed, "<test-{now/d}>::failures"), equalTo(new ResolvedExpression("<test-{now/d}>", FAILURES)));
        // Selectors inside of date math expressions will trip an exception because they do not match an existing component name exactly
        expectThrows(InvalidIndexNameException.class, () -> resolve(selectorsAllowed, "<test-{now/d}::failures>"));

        // === Corner Cases
        // Empty index name is not necessarily disallowed, but will be filtered out in the next steps of resolution
        assertThat(resolve(selectorsAllowed, "::data"), equalTo(new ResolvedExpression("", DATA)));
        // Remote cluster syntax is respected, even if code higher up the call stack is likely to already have handled it already
        assertThat(resolve(selectorsAllowed, "cluster:index::data"), equalTo(new ResolvedExpression("cluster:index", DATA)));
        // CCS with an empty index name is not necessarily disallowed, though other code in the resolution logic will likely throw
        assertThat(resolve(selectorsAllowed, "cluster:::data"), equalTo(new ResolvedExpression("cluster:", DATA)));
        // Same for empty cluster and index names
        assertThat(resolve(selectorsAllowed, ":::data"), equalTo(new ResolvedExpression(":", DATA)));
        // Any more prefix colon characters will trigger the multiple separators error logic
        expectThrows(InvalidIndexNameException.class, () -> resolve(selectorsAllowed, "::::data"));
        // Suffix case is not supported because there is no component named with the empty string
        expectThrows(InvalidIndexNameException.class, () -> resolve(selectorsAllowed, "index::"));
    }

    public void testResolveMatchAllToSelectors() {
        Context selectorsAllowed = getContext(getOptionsForSelectors());
        assertThat(resolveMatchAllToSelector(selectorsAllowed, "*"), is(DATA));
        assertThat(resolveMatchAllToSelector(selectorsAllowed, "*::data"), is(DATA));
        assertThat(resolveMatchAllToSelector(selectorsAllowed, "*::failures"), is(FAILURES));
        assertThat(resolveMatchAllToSelector(selectorsAllowed, "_all"), is(DATA));
        assertThat(resolveMatchAllToSelector(selectorsAllowed, "_all::data"), is(DATA));
        assertThat(resolveMatchAllToSelector(selectorsAllowed, "_all::failures"), is(FAILURES));

        Context selectorsDisallowed = getContext(getDisabledSelectorOptions());
        assertThat(resolveMatchAllToSelector(selectorsDisallowed, "*"), is(nullValue()));
        expectThrows(IllegalArgumentException.class, () -> resolveMatchAllToSelector(selectorsDisallowed, "*::data"));
        expectThrows(IllegalArgumentException.class, () -> resolveMatchAllToSelector(selectorsDisallowed, "*::failures"));
        assertThat(resolveMatchAllToSelector(selectorsDisallowed, "_all"), is(nullValue()));
        expectThrows(IllegalArgumentException.class, () -> resolveMatchAllToSelector(selectorsDisallowed, "_all::data"));
        expectThrows(IllegalArgumentException.class, () -> resolveMatchAllToSelector(selectorsDisallowed, "_all::failures"));
    }

    public void testCombineExpressionWithSelector() {
        expectThrows(NullPointerException.class, () -> IndexNameExpressionResolver.combineSelectorExpression(null, null));
        expectThrows(NullPointerException.class, () -> IndexNameExpressionResolver.combineSelectorExpression(null, ""));
        expectThrows(NullPointerException.class, () -> IndexNameExpressionResolver.combineSelectorExpression(null, "a"));
        expectThrows(NullPointerException.class, () -> IndexNameExpressionResolver.combineSelectorExpression(null, "*"));
        assertThat(IndexNameExpressionResolver.combineSelectorExpression("", null), is(equalTo("")));
        assertThat(IndexNameExpressionResolver.combineSelectorExpression("", ""), is(equalTo("::")));
        assertThat(IndexNameExpressionResolver.combineSelectorExpression("a", null), is(equalTo("a")));
        assertThat(IndexNameExpressionResolver.combineSelectorExpression("a", ""), is(equalTo("a::")));
        assertThat(IndexNameExpressionResolver.combineSelectorExpression("a", "b"), is(equalTo("a::b")));
        assertThat(IndexNameExpressionResolver.combineSelectorExpression("a", "*"), is(equalTo("a::*")));
        assertThat(IndexNameExpressionResolver.combineSelectorExpression("*", "b"), is(equalTo("*::b")));
        assertThat(IndexNameExpressionResolver.combineSelectorExpression("*", "*"), is(equalTo("*::*")));
    }

    public void testHasSelectorSuffix() {
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix(null), is(false));
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix(""), is(false));
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix("abcdefg"), is(false));
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix("*"), is(false));
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix("cluster:index"), is(false));
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix("index::data"), is(true));
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix("index::failures"), is(true));
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix("index::any"), is(true));
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix("index::*"), is(true));
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix("index::::::::::toomany"), is(true));
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix("cluster:index::data"), is(true));
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix("*:*::*"), is(true));
        assertThat(IndexNameExpressionResolver.hasSelectorSuffix("index::value::value"), is(true));
    }

    public void testSplitSelectorExpression() {
        expectThrows(NullPointerException.class, () -> IndexNameExpressionResolver.splitSelectorExpression(null));
        assertThat(IndexNameExpressionResolver.splitSelectorExpression(""), is(equalTo(new Tuple<>("", null))));
        assertThat(IndexNameExpressionResolver.splitSelectorExpression("a"), is(equalTo(new Tuple<>("a", null))));
        assertThat(IndexNameExpressionResolver.splitSelectorExpression("*"), is(equalTo(new Tuple<>("*", null))));
        assertThat(IndexNameExpressionResolver.splitSelectorExpression("index"), is(equalTo(new Tuple<>("index", null))));
        assertThat(IndexNameExpressionResolver.splitSelectorExpression("cluster:index"), is(equalTo(new Tuple<>("cluster:index", null))));
        assertThat(IndexNameExpressionResolver.splitSelectorExpression("*:index"), is(equalTo(new Tuple<>("*:index", null))));
        assertThat(IndexNameExpressionResolver.splitSelectorExpression("cluster:*"), is(equalTo(new Tuple<>("cluster:*", null))));
        assertThat(IndexNameExpressionResolver.splitSelectorExpression("*:*"), is(equalTo(new Tuple<>("*:*", null))));
        assertThat(IndexNameExpressionResolver.splitSelectorExpression("*:*:*"), is(equalTo(new Tuple<>("*:*:*", null))));

        assertThat(IndexNameExpressionResolver.splitSelectorExpression("a::data"), is(equalTo(new Tuple<>("a", "data"))));
        assertThat(IndexNameExpressionResolver.splitSelectorExpression("a::failures"), is(equalTo(new Tuple<>("a", "failures"))));
        assertThat(IndexNameExpressionResolver.splitSelectorExpression("a::*"), is(equalTo(new Tuple<>("a", "*"))));
        expectThrows(InvalidIndexNameException.class, () -> IndexNameExpressionResolver.splitSelectorExpression("a::random"));
        expectThrows(InvalidIndexNameException.class, () -> IndexNameExpressionResolver.splitSelectorExpression("a::d*ta"));
        expectThrows(InvalidIndexNameException.class, () -> IndexNameExpressionResolver.splitSelectorExpression("a::*ailures"));
        expectThrows(InvalidIndexNameException.class, () -> IndexNameExpressionResolver.splitSelectorExpression("a::"));
        expectThrows(InvalidIndexNameException.class, () -> IndexNameExpressionResolver.splitSelectorExpression("a::**"));
        expectThrows(InvalidIndexNameException.class, () -> IndexNameExpressionResolver.splitSelectorExpression("index::data::*"));
        assertThat(IndexNameExpressionResolver.splitSelectorExpression("::*"), is(equalTo(new Tuple<>("", "*"))));
    }

    private static IndicesOptions getOptionsForSelectors() {
        return IndicesOptions.builder().gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(true)).build();
    }

    private static IndicesOptions getDisabledSelectorOptions() {
        return IndicesOptions.builder().gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(false)).build();
    }

    private static Context getContext(IndicesOptions indicesOptions) {
        return new Context(mock(ClusterState.class), indicesOptions, SystemIndices.SystemIndexAccessLevel.NONE);
    }

    private static ResolvedExpression resolve(Context context, String expression) {
        return SelectorResolver.parseExpression(expression, context.getOptions());
    }

    private static IndexComponentSelector resolveMatchAllToSelector(Context context, String matchAll) {
        return SelectorResolver.parseMatchAllToSelector(context, matchAll);
    }
}
