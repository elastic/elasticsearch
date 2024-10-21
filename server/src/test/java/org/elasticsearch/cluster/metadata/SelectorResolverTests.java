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

import java.util.List;

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
        // Allow selectors TRUE and default selector of $data (example, a search API)
        Context dataSelector = getContext(getOptionsForSelectors(DATA));

        assertThat(SelectorResolver.resolve(dataSelector, "testXXX"), equalTo(new ResolvedExpression("testXXX", DATA)));
        assertThat(SelectorResolver.resolve(dataSelector, "testXXX::data"), equalTo(new ResolvedExpression("testXXX", DATA)));
        assertThat(SelectorResolver.resolve(dataSelector, "testXXX::failures"), equalTo(new ResolvedExpression("testXXX", FAILURES)));
        assertThat(SelectorResolver.resolve(dataSelector, "testXXX::*"), equalTo(new ResolvedExpression("testXXX", ALL_APPLICABLE)));

        // Allow selectors TRUE and default selector of $failures
        Context failuresSelector = getContext(getOptionsForSelectors(FAILURES));

        assertThat(SelectorResolver.resolve(failuresSelector, "testXXX"), equalTo(new ResolvedExpression("testXXX", FAILURES)));
        assertThat(SelectorResolver.resolve(failuresSelector, "testXXX::data"), equalTo(new ResolvedExpression("testXXX", DATA)));
        assertThat(SelectorResolver.resolve(failuresSelector, "testXXX::failures"), equalTo(new ResolvedExpression("testXXX", FAILURES)));
        assertThat(SelectorResolver.resolve(failuresSelector, "testXXX::*"), equalTo(new ResolvedExpression("testXXX", ALL_APPLICABLE)));

        // Allow selectors TRUE and default selectors of both ::* (example, a management/monitoring API)
        Context bothSelectors = getContext(getOptionsForSelectors(ALL_APPLICABLE));

        assertThat(SelectorResolver.resolve(bothSelectors, "testXXX"), equalTo(new ResolvedExpression("testXXX", ALL_APPLICABLE)));
        assertThat(SelectorResolver.resolve(bothSelectors, "testXXX::data"), equalTo(new ResolvedExpression("testXXX", DATA)));
        assertThat(SelectorResolver.resolve(bothSelectors, "testXXX::failures"), equalTo(new ResolvedExpression("testXXX", FAILURES)));
        assertThat(SelectorResolver.resolve(bothSelectors, "testXXX::*"), equalTo(new ResolvedExpression("testXXX", ALL_APPLICABLE)));

        // Disallow selectors (example: creating, modifying, or deleting indices/data streams/aliases).
        // Accepts standard expressions but throws when selectors are specified.
        Context noSelectors = getContext(getDisabledSelectorOptions());

        assertThat(SelectorResolver.resolve(noSelectors, "testXXX"), equalTo(new ResolvedExpression("testXXX")));
        expectThrows(IllegalArgumentException.class, () -> SelectorResolver.resolve(noSelectors, "testXXX::data"));
        expectThrows(IllegalArgumentException.class, () -> SelectorResolver.resolve(noSelectors, "testXXX::failures"));
        expectThrows(IllegalArgumentException.class, () -> SelectorResolver.resolve(noSelectors, "testXXX::*"));

        // === Errors
        // Only recognized components can be selected
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.resolve(dataSelector, "testXXX::custom"));
        // Spelling is important
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.resolve(dataSelector, "testXXX::failres"));
        // Only the match all wildcard is supported
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.resolve(dataSelector, "testXXX::d*ta"));
        // Only one selector separator is allowed per expression
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.resolve(dataSelector, "test::XXX::data"));

        // === Wildcards, Date Math, and edge cases
        // Wildcards are left as-is (handled in wildcard resolver)
        assertThat(SelectorResolver.resolve(dataSelector, "*"), equalTo(new ResolvedExpression("*", DATA)));
        // Exclusions are left as-is (handled in wildcard resolver)
        assertThat(SelectorResolver.resolve(dataSelector, "-testXXX"), equalTo(new ResolvedExpression("-testXXX", DATA)));
        // Exclusions with selectors will have the selectors parsed
        assertThat(SelectorResolver.resolve(dataSelector, "-testXXX::failures"), equalTo(new ResolvedExpression("-testXXX", FAILURES)));
        // Date math is left unprocessed (handled in date math resolver)
        assertThat(SelectorResolver.resolve(dataSelector, "<test-{now/d}>"), equalTo(new ResolvedExpression("<test-{now/d}>", DATA)));
        // Providing a selector requires adding after the date math brackets
        assertThat(
            SelectorResolver.resolve(dataSelector, "<test-{now/d}>::failures"),
            equalTo(new ResolvedExpression("<test-{now/d}>", FAILURES))
        );
        // Selectors inside of date math expressions will trip an exception because they do not match an existing component name exactly
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.resolve(dataSelector, "<test-{now/d}::failures>"));

        // === Corner Cases
        // Empty index name is not necessarily disallowed, but will be filtered out in the next steps of resolution
        assertThat(SelectorResolver.resolve(dataSelector, "::data"), equalTo(new ResolvedExpression("", DATA)));
        // Remote cluster syntax is respected, even if code higher up the call stack is likely to already have handled it already
        assertThat(SelectorResolver.resolve(dataSelector, "cluster:index::data"), equalTo(new ResolvedExpression("cluster:index", DATA)));
        // CCS with an empty index name is not necessarily disallowed, though other code in the resolution logic will likely throw
        assertThat(SelectorResolver.resolve(dataSelector, "cluster:::data"), equalTo(new ResolvedExpression("cluster:", DATA)));
        // Same for empty cluster and index names
        assertThat(SelectorResolver.resolve(dataSelector, ":::data"), equalTo(new ResolvedExpression(":", DATA)));
        // Any more prefix colon characters will trigger the multiple separators error logic
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.resolve(dataSelector, "::::data"));
        // Suffix case is not supported because there is no component named with the empty string
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.resolve(dataSelector, "index::"));
    }

    public void testMultipleResolutions() {
        Context dataSelector = getContext(getOptionsForSelectors(DATA));
        assertThat(
            resolveAll(dataSelector, "testXXX", "testYYY", "testZZZ"),
            is(
                List.of(
                    new ResolvedExpression("testXXX", DATA),
                    new ResolvedExpression("testYYY", DATA),
                    new ResolvedExpression("testZZZ", DATA)
                )
            )
        );
        assertThat(
            resolveAll(dataSelector, "testXXX::data", "testYYY::failures", "testZZZ::*"),
            is(
                List.of(
                    new ResolvedExpression("testXXX", DATA),
                    new ResolvedExpression("testYYY", FAILURES),
                    new ResolvedExpression("testZZZ", ALL_APPLICABLE)
                )
            )
        );
        Context bothSelectors = getContext(getOptionsForSelectors(ALL_APPLICABLE));
        assertThat(
            resolveAll(bothSelectors, "*", "testYYY::failures", "testZZZ::*"),
            is(
                List.of(
                    new ResolvedExpression("*", ALL_APPLICABLE),
                    new ResolvedExpression("testYYY", FAILURES),
                    new ResolvedExpression("testZZZ", ALL_APPLICABLE)
                )
            )
        );
        Context noSelectors = getContext(getDisabledSelectorOptions());
        expectThrows(IllegalArgumentException.class, () -> resolveAll(noSelectors, "*", "testXXX::failures"));
        assertThat(
            resolveAll(noSelectors, "*", "testXXX"),
            is(List.of(new ResolvedExpression("*", null), new ResolvedExpression("testXXX", null)))
        );
    }

    public void testResolveMatchAllToSelectors() {
        Context dataSelector = getContext(getOptionsForSelectors(DATA));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(dataSelector, "*"), is(DATA));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(dataSelector, "*::data"), is(DATA));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(dataSelector, "*::failures"), is(FAILURES));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(dataSelector, "_all"), is(DATA));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(dataSelector, "_all::data"), is(DATA));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(dataSelector, "_all::failures"), is(FAILURES));

        Context bothSelector = getContext(getOptionsForSelectors(ALL_APPLICABLE));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(bothSelector, "*"), is(ALL_APPLICABLE));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(bothSelector, "*::data"), is(DATA));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(bothSelector, "*::failures"), is(FAILURES));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(bothSelector, "_all"), is(ALL_APPLICABLE));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(bothSelector, "_all::data"), is(DATA));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(bothSelector, "_all::failures"), is(FAILURES));

        Context noneSelector = getContext(getDisabledSelectorOptions());
        assertThat(SelectorResolver.resolveMatchAllToSelectors(noneSelector, "*"), nullValue());
        expectThrows(IllegalArgumentException.class, () -> SelectorResolver.resolveMatchAllToSelectors(noneSelector, "*::data"));
        expectThrows(IllegalArgumentException.class, () -> SelectorResolver.resolveMatchAllToSelectors(noneSelector, "*::failures"));
        assertThat(SelectorResolver.resolveMatchAllToSelectors(noneSelector, "_all"), is(nullValue()));
        expectThrows(IllegalArgumentException.class, () -> SelectorResolver.resolveMatchAllToSelectors(noneSelector, "_all::data"));
        expectThrows(IllegalArgumentException.class, () -> SelectorResolver.resolveMatchAllToSelectors(noneSelector, "_all::failures"));
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

    private static IndicesOptions getOptionsForSelectors(IndexComponentSelector selector) {
        return IndicesOptions.builder()
            .selectorOptions(IndicesOptions.SelectorOptions.builder().defaultSelectors(selector))
            .gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(true))
            .build();
    }

    private static IndicesOptions getDisabledSelectorOptions() {
        return IndicesOptions.builder().gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(false)).build();
    }

    private static Context getContext(IndicesOptions indicesOptions) {
        return new Context(mock(ClusterState.class), indicesOptions, SystemIndices.SystemIndexAccessLevel.NONE);
    }

    private static List<ResolvedExpression> resolveAll(Context context, String... expressions) {
        return SelectorResolver.resolve(context, List.of(expressions));
    }
}
