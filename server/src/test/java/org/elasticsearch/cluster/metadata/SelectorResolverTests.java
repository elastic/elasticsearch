/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use thfile excet
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SelectorResolver;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.elasticsearch.action.support.IndexComponentSelector.DATA;
import static org.elasticsearch.action.support.IndexComponentSelector.FAILURES;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.Context;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class SelectorResolverTests extends ESTestCase {

    public void testResolveExpression() {
        // === Parsing and defaults
        // Allow selectors TRUE and default selector of $data (example, a search API)
        Context dataSelector = getContext(getOptionsForSelectors(DATA));

        assertThat(resolve(dataSelector, "testXXX"), equalTo(List.of(new ResolvedExpression("testXXX", DATA))));
        assertThat(resolve(dataSelector, "testXXX::data"), equalTo(List.of(new ResolvedExpression("testXXX", DATA))));
        assertThat(resolve(dataSelector, "testXXX::failures"), equalTo(List.of(new ResolvedExpression("testXXX", FAILURES))));
        assertThat(
            resolve(dataSelector, "testXXX::*"),
            equalTo(List.of(new ResolvedExpression("testXXX", DATA), new ResolvedExpression("testXXX", FAILURES)))
        );

        // Allow selectors TRUE and default selector of $failures
        Context failuresSelector = getContext(getOptionsForSelectors(FAILURES));

        assertThat(resolve(failuresSelector, "testXXX"), equalTo(List.of(new ResolvedExpression("testXXX", FAILURES))));
        assertThat(resolve(failuresSelector, "testXXX::data"), equalTo(List.of(new ResolvedExpression("testXXX", DATA))));
        assertThat(resolve(failuresSelector, "testXXX::failures"), equalTo(List.of(new ResolvedExpression("testXXX", FAILURES))));
        assertThat(
            resolve(failuresSelector, "testXXX::*"),
            equalTo(List.of(new ResolvedExpression("testXXX", DATA), new ResolvedExpression("testXXX", FAILURES)))
        );

        // Allow selectors TRUE and default selectors of both $data and $failures (example, a management/monitoring API)
        Context bothSelectors = getContext(getOptionsForSelectors(DATA, FAILURES));

        assertThat(
            resolve(bothSelectors, "testXXX"),
            equalTo(List.of(new ResolvedExpression("testXXX", DATA), new ResolvedExpression("testXXX", FAILURES)))
        );
        assertThat(resolve(bothSelectors, "testXXX::data"), equalTo(List.of(new ResolvedExpression("testXXX", DATA))));
        assertThat(resolve(bothSelectors, "testXXX::failures"), equalTo(List.of(new ResolvedExpression("testXXX", FAILURES))));
        assertThat(
            resolve(bothSelectors, "testXXX::*"),
            equalTo(List.of(new ResolvedExpression("testXXX", DATA), new ResolvedExpression("testXXX", FAILURES)))
        );

        // Disallow selectors (example: creating, modifying, or deleting indices/data streams/aliases).
        // Accepts standard expressions but throws when selectors are specified.
        Context noSelectors = getContext(getDisabledSelectorOptions());

        assertThat(resolve(noSelectors, "testXXX"), equalTo(List.of(new ResolvedExpression("testXXX"))));
        expectThrows(IllegalArgumentException.class, () -> resolve(noSelectors, "testXXX::data"));
        expectThrows(IllegalArgumentException.class, () -> resolve(noSelectors, "testXXX::failures"));
        expectThrows(IllegalArgumentException.class, () -> resolve(noSelectors, "testXXX::*"));

        // === Errors
        // Only recognized components can be selected
        expectThrows(InvalidIndexNameException.class, () -> resolve(dataSelector, "testXXX::custom"));
        // Spelling is important
        expectThrows(InvalidIndexNameException.class, () -> resolve(dataSelector, "testXXX::failres"));
        // Only the match all wildcard is supported
        expectThrows(InvalidIndexNameException.class, () -> resolve(dataSelector, "testXXX::d*ta"));
        // Only one selector separator is allowed per expression
        expectThrows(InvalidIndexNameException.class, () -> resolve(dataSelector, "test::XXX::data"));

        // === Wildcards, Date Math, and edge cases
        // Wildcards are left as-is (handled in wildcard resolver)
        assertThat(resolve(dataSelector, "*"), equalTo(List.of(new ResolvedExpression("*", DATA))));
        // Exclusions are left as-is (handled in wildcard resolver)
        assertThat(resolve(dataSelector, "-testXXX"), equalTo(List.of(new ResolvedExpression("-testXXX", DATA))));
        // Exclusions with selectors will have the selectors parsed
        assertThat(resolve(dataSelector, "-testXXX::failures"), equalTo(List.of(new ResolvedExpression("-testXXX", FAILURES))));
        // Date math is left unprocessed (handled in date math resolver)
        assertThat(resolve(dataSelector, "<test-{now/d}>"), equalTo(List.of(new ResolvedExpression("<test-{now/d}>", DATA))));
        // Providing a selector requires adding after the date math brackets
        assertThat(
            resolve(dataSelector, "<test-{now/d}>::failures"),
            equalTo(List.of(new ResolvedExpression("<test-{now/d}>", FAILURES)))
        );
        // Selectors inside of date math expressions will trip an exception because they do not match an existing component name exactly
        expectThrows(InvalidIndexNameException.class, () -> resolve(dataSelector, "<test-{now/d}::failures>"));

        // === Corner Cases
        // Empty index name is not necessarily disallowed, but will be filtered out in the next steps of resolution
        assertThat(resolve(dataSelector, "::data"), equalTo(List.of(new ResolvedExpression("", DATA))));
        // Remote cluster syntax is respected, even if code higher up the call stack is likely to already have handled it already
        assertThat(resolve(dataSelector, "cluster:index::data"), equalTo(List.of(new ResolvedExpression("cluster:index", DATA))));
        // CCS with an empty index name is not necessarily disallowed, though other code in the resolution logic will likely throw
        assertThat(resolve(dataSelector, "cluster:::data"), equalTo(List.of(new ResolvedExpression("cluster:", DATA))));
        // Same for empty cluster and index names
        assertThat(resolve(dataSelector, ":::data"), equalTo(List.of(new ResolvedExpression(":", DATA))));
        // Any more prefix colon characters will trigger the multiple separators error logic
        expectThrows(InvalidIndexNameException.class, () -> resolve(dataSelector, "::::data"));
        // Suffix case is not supported because there is no component named with the empty string
        expectThrows(InvalidIndexNameException.class, () -> resolve(dataSelector, "index::"));
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
                    new ResolvedExpression("testZZZ", DATA),
                    new ResolvedExpression("testZZZ", FAILURES)
                )
            )
        );
        Context bothSelectors = getContext(getOptionsForSelectors(DATA, FAILURES));
        assertThat(
            resolveAll(bothSelectors, "*", "testYYY::failures", "testZZZ::*"),
            is(
                List.of(
                    new ResolvedExpression("*", DATA),
                    new ResolvedExpression("*", FAILURES),
                    new ResolvedExpression("testYYY", FAILURES),
                    new ResolvedExpression("testZZZ", DATA),
                    new ResolvedExpression("testZZZ", FAILURES)
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
        assertThat(resolveMatchAllToSelectors(dataSelector, "*"), is(EnumSet.of(DATA)));
        assertThat(resolveMatchAllToSelectors(dataSelector, "*::data"), is(EnumSet.of(DATA)));
        assertThat(resolveMatchAllToSelectors(dataSelector, "*::failures"), is(EnumSet.of(FAILURES)));
        assertThat(resolveMatchAllToSelectors(dataSelector, "_all"), is(EnumSet.of(DATA)));
        assertThat(resolveMatchAllToSelectors(dataSelector, "_all::data"), is(EnumSet.of(DATA)));
        assertThat(resolveMatchAllToSelectors(dataSelector, "_all::failures"), is(EnumSet.of(FAILURES)));

        Context bothSelector = getContext(getOptionsForSelectors(DATA, FAILURES));
        assertThat(resolveMatchAllToSelectors(bothSelector, "*"), is(EnumSet.of(DATA, FAILURES)));
        assertThat(resolveMatchAllToSelectors(bothSelector, "*::data"), is(EnumSet.of(DATA)));
        assertThat(resolveMatchAllToSelectors(bothSelector, "*::failures"), is(EnumSet.of(FAILURES)));
        assertThat(resolveMatchAllToSelectors(bothSelector, "_all"), is(EnumSet.of(DATA, FAILURES)));
        assertThat(resolveMatchAllToSelectors(bothSelector, "_all::data"), is(EnumSet.of(DATA)));
        assertThat(resolveMatchAllToSelectors(bothSelector, "_all::failures"), is(EnumSet.of(FAILURES)));

        Context noneSelector = getContext(getDisabledSelectorOptions());
        assertThat(resolveMatchAllToSelectors(noneSelector, "*"), is(EnumSet.noneOf(IndexComponentSelector.class)));
        expectThrows(IllegalArgumentException.class, () -> resolveMatchAllToSelectors(noneSelector, "*::data"));
        expectThrows(IllegalArgumentException.class, () -> resolveMatchAllToSelectors(noneSelector, "*::failures"));
        assertThat(resolveMatchAllToSelectors(noneSelector, "_all"), is(EnumSet.noneOf(IndexComponentSelector.class)));
        expectThrows(IllegalArgumentException.class, () -> resolveMatchAllToSelectors(noneSelector, "_all::data"));
        expectThrows(IllegalArgumentException.class, () -> resolveMatchAllToSelectors(noneSelector, "_all::failures"));
    }

    private static IndicesOptions getOptionsForSelectors(IndexComponentSelector... selectors) {
        return IndicesOptions.builder()
            .selectorOptions(IndicesOptions.SelectorOptions.builder().setDefaultSelectors(EnumSet.copyOf(Arrays.asList(selectors))))
            .gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(true))
            .build();
    }

    private static IndicesOptions getDisabledSelectorOptions() {
        return IndicesOptions.builder().gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(false)).build();
    }

    private static Context getContext(IndicesOptions indicesOptions) {
        return new Context(mock(ClusterState.class), indicesOptions, SystemIndices.SystemIndexAccessLevel.NONE);
    }

    private static List<ResolvedExpression> resolve(Context context, String expression) {
        return SelectorResolver.resolve(context, expression).toList();
    }

    private static List<ResolvedExpression> resolveAll(Context context, String... expressions) {
        return SelectorResolver.resolve(context, List.of(expressions));
    }

    private static EnumSet<IndexComponentSelector> resolveMatchAllToSelectors(Context context, String matchAll) {
        return SelectorResolver.resolveMatchAllToSelectors(context, matchAll);
    }
}
