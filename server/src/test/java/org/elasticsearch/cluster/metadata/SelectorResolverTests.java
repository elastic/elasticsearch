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
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SelectorResolver;

import static org.elasticsearch.action.support.IndexComponentSelector.DATA;
import static org.elasticsearch.action.support.IndexComponentSelector.FAILURES;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.Context;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class SelectorResolverTests extends ESTestCase {

    public void testResolveExpression() {
        // Allow selectors TRUE and default selector of $data (example, a search API)
        Context dataSelector = getContext(getOptionsForSelectors(DATA));

        assertThat(resolve(dataSelector, "testXXX"), equalTo(List.of(new ResolvedExpression("testXXX", DATA))));
        assertThat(resolve(dataSelector, "testXXX$data"), equalTo(List.of(new ResolvedExpression("testXXX", DATA))));
        assertThat(resolve(dataSelector, "testXXX$failures"), equalTo(List.of(new ResolvedExpression("testXXX", FAILURES))));

        // Allow selectors TRUE and default selector of $failures
        Context failuresSelector = getContext(getOptionsForSelectors(FAILURES));

        assertThat(resolve(failuresSelector, "testXXX"), equalTo(List.of(new ResolvedExpression("testXXX", FAILURES))));
        assertThat(resolve(failuresSelector, "testXXX$data"), equalTo(List.of(new ResolvedExpression("testXXX", DATA))));
        assertThat(resolve(failuresSelector, "testXXX$failures"), equalTo(List.of(new ResolvedExpression("testXXX", FAILURES))));

        // Allow selectors TRUE and default selectors of both $data and $failures (example, a management/monitoring API)
        Context bothSelectors = getContext(getOptionsForSelectors(DATA, FAILURES));

        assertThat(
            resolve(bothSelectors, "testXXX"),
            equalTo(List.of(new ResolvedExpression("testXXX", DATA), new ResolvedExpression("testXXX", FAILURES)))
        );
        assertThat(resolve(bothSelectors, "testXXX$data"), equalTo(List.of(new ResolvedExpression("testXXX", DATA))));
        assertThat(resolve(bothSelectors, "testXXX$failures"), equalTo(List.of(new ResolvedExpression("testXXX", FAILURES))));

        // Disallow selectors (example: creating, modifying, or deleting indices/data streams/aliases).
        // Results in expressions with no selector values.
        Context noSelectors = getContext(getDisabledSelectorOptions());

        assertThat(resolve(noSelectors, "testXXX"), equalTo(List.of(new ResolvedExpression("testXXX"))));
        assertThat(resolve(noSelectors, "testXXX$data"), equalTo(List.of(new ResolvedExpression("testXXX$data"))));
        assertThat(resolve(noSelectors, "testXXX$failures"), equalTo(List.of(new ResolvedExpression("testXXX$failures"))));

        // Wildcards, Date Math, and edge cases
        // Wildcards are left as-is (handled in wildcard resolver)
        assertThat(resolve(dataSelector, "*"), equalTo(List.of(new ResolvedExpression("*", DATA))));
        // Exclusions are left as-is (handled in wildcard resolver)
        assertThat(resolve(dataSelector, "-testXXX"), equalTo(List.of(new ResolvedExpression("-testXXX", DATA))));
        // Exclusions with selectors will have the selectors parsed
        assertThat(resolve(dataSelector, "-testXXX$failures"), equalTo(List.of(new ResolvedExpression("-testXXX", FAILURES))));
        // Date math is left unprocessed (handled in date math resolver)
        assertThat(resolve(dataSelector, "<test-{now/d}>"), equalTo(List.of(new ResolvedExpression("<test-{now/d}>", DATA))));
        // Providing a selector requires adding after the date math brackets
        assertThat(
            resolve(dataSelector, "<test-{now/d}>$failures"),
            equalTo(List.of(new ResolvedExpression("<test-{now/d}>", FAILURES)))
        );
        // Selectors inside of date math expressions are left in as part of the index name and are not parsed
        assertThat(
            resolve(dataSelector, "<test-{now/d}$failures>"),
            equalTo(List.of(new ResolvedExpression("<test-{now/d}$failures>", DATA)))
        );
        // Misspelling the suffix on a date math expression will result in it not being parsed off. This will break the date math detection
        // logic because it no longer ends in a > character.
        assertThat(
            resolve(dataSelector, "<test-{now/d}>$failrues"), // misspelled
            equalTo(List.of(new ResolvedExpression("<test-{now/d}>$failrues", DATA)))
        );
        // custom is not a recognized selector
        assertThat(resolve(dataSelector, "testXXX$custom"), equalTo(List.of(new ResolvedExpression("testXXX$custom", DATA))));
        // d* is not a recognized selector
        assertThat(resolve(dataSelector, "testXXX$d*"), equalTo(List.of(new ResolvedExpression("testXXX$d*", DATA))));
        // The last $data is parsed, leaving the first $data as part of the remaining expression
        assertThat(resolve(dataSelector, "testXXX$data$data"), equalTo(List.of(new ResolvedExpression("testXXX$data", DATA))));
        // The last $data is parsed, leaving the first $failures as part of the remaining expression
        assertThat(resolve(dataSelector, "testXXX$failures$data"), equalTo(List.of(new ResolvedExpression("testXXX$failures", DATA))));
        // the last $failures is parsed, leaving the first $data as part of the remaining expression
        assertThat(resolve(dataSelector, "testXXX$data$failures"), equalTo(List.of(new ResolvedExpression("testXXX$data", FAILURES))));
        // the last $failures is parsed, leaving the first $failures as part of the remaining expression
        assertThat(
            resolve(dataSelector, "testXXX$failures$failures"),
            equalTo(List.of(new ResolvedExpression("testXXX$failures", FAILURES)))
        );
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
            resolveAll(dataSelector, "testXXX$data", "testYYY$failures", "testZZZ$data$data", "<test-{now}>$failures", "test$custom"),
            is(
                List.of(
                    new ResolvedExpression("testXXX", DATA),
                    new ResolvedExpression("testYYY", FAILURES),
                    new ResolvedExpression("testZZZ$data", DATA),
                    new ResolvedExpression("<test-{now}>", FAILURES),
                    new ResolvedExpression("test$custom", DATA)
                )
            )
        );
        Context bothSelectors = getContext(getOptionsForSelectors(DATA, FAILURES));
        assertThat(
            resolveAll(bothSelectors, "*", "testYYY$failures", "testZZZ$data$data", "<test-{now}>$failures", "test$custom"),
            is(
                List.of(
                    new ResolvedExpression("*", DATA),
                    new ResolvedExpression("*", FAILURES),
                    new ResolvedExpression("testYYY", FAILURES),
                    new ResolvedExpression("testZZZ$data", DATA),
                    new ResolvedExpression("<test-{now}>", FAILURES),
                    new ResolvedExpression("test$custom", DATA),
                    new ResolvedExpression("test$custom", FAILURES)
                )
            )
        );
        Context noSelectors = getContext(getDisabledSelectorOptions());
        assertThat(
            resolveAll(noSelectors, "*", "testYYY$failures", "testZZZ$data$data", "<test-{now}>$failures", "test$custom"),
            is(
                List.of(
                    new ResolvedExpression("*", null),
                    new ResolvedExpression("testYYY$failures", null),
                    new ResolvedExpression("testZZZ$data$data", null),
                    new ResolvedExpression("<test-{now}>$failures", null),
                    new ResolvedExpression("test$custom", null)
                )
            )
        );
    }

    public void testResolveMatchAllToSelectors() {
        Context dataSelector = getContext(getOptionsForSelectors(DATA));
        assertThat(resolveMatchAllToSelectors(dataSelector, "*"), is(EnumSet.of(DATA)));
        assertThat(resolveMatchAllToSelectors(dataSelector, "*$data"), is(EnumSet.of(DATA)));
        assertThat(resolveMatchAllToSelectors(dataSelector, "*$failures"), is(EnumSet.of(FAILURES)));
        assertThat(resolveMatchAllToSelectors(dataSelector, "_all"), is(EnumSet.of(DATA)));
        assertThat(resolveMatchAllToSelectors(dataSelector, "_all$data"), is(EnumSet.of(DATA)));
        assertThat(resolveMatchAllToSelectors(dataSelector, "_all$failures"), is(EnumSet.of(FAILURES)));

        Context bothSelector = getContext(getOptionsForSelectors(DATA, FAILURES));
        assertThat(resolveMatchAllToSelectors(bothSelector, "*"), is(EnumSet.of(DATA, FAILURES)));
        assertThat(resolveMatchAllToSelectors(bothSelector, "*$data"), is(EnumSet.of(DATA)));
        assertThat(resolveMatchAllToSelectors(bothSelector, "*$failures"), is(EnumSet.of(FAILURES)));
        assertThat(resolveMatchAllToSelectors(bothSelector, "_all"), is(EnumSet.of(DATA, FAILURES)));
        assertThat(resolveMatchAllToSelectors(bothSelector, "_all$data"), is(EnumSet.of(DATA)));
        assertThat(resolveMatchAllToSelectors(bothSelector, "_all$failures"), is(EnumSet.of(FAILURES)));

        Context noneSelector = getContext(getDisabledSelectorOptions());
        assertThat(resolveMatchAllToSelectors(noneSelector, "*"), is(EnumSet.noneOf(IndexComponentSelector.class)));
        assertThat(resolveMatchAllToSelectors(noneSelector, "*$data"), is(EnumSet.noneOf(IndexComponentSelector.class)));
        assertThat(resolveMatchAllToSelectors(noneSelector, "*$failures"), is(EnumSet.noneOf(IndexComponentSelector.class)));
        assertThat(resolveMatchAllToSelectors(noneSelector, "_all"), is(EnumSet.noneOf(IndexComponentSelector.class)));
        assertThat(resolveMatchAllToSelectors(noneSelector, "_all$data"), is(EnumSet.noneOf(IndexComponentSelector.class)));
        assertThat(resolveMatchAllToSelectors(noneSelector, "_all$failures"), is(EnumSet.noneOf(IndexComponentSelector.class)));
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
