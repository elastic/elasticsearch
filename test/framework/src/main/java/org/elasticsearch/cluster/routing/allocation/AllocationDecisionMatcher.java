/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/// Matcher for [Decision] objects
public class AllocationDecisionMatcher extends BaseMatcher<Decision> {

    private final Decision.Type expectedType;
    private final Matcher<String> expectedLabel;
    private final Matcher<String> explanationMatcher;

    public AllocationDecisionMatcher(Decision.Type expectedType, Matcher<String> expectedLabel, Matcher<String> explanationMatcher) {
        this.expectedType = expectedType;
        this.expectedLabel = expectedLabel;
        this.explanationMatcher = explanationMatcher;
    }

    public static AllocationDecisionMatcher isNoDecision() {
        return new AllocationDecisionMatcher(Decision.Type.NO, any(String.class), anyOf(any(String.class), nullValue(String.class)));
    }

    public static AllocationDecisionMatcher isNoDecision(String expectedLabel) {
        return new AllocationDecisionMatcher(Decision.Type.NO, equalTo(expectedLabel), anyOf(any(String.class), nullValue(String.class)));
    }

    public static AllocationDecisionMatcher isNoDecisionWithNoExplanation(String expectedLabel) {
        return new AllocationDecisionMatcher(Decision.Type.NO, equalTo(expectedLabel), nullValue(String.class));
    }

    public static AllocationDecisionMatcher isNoDecisionWithExplanationMatching(String expectedLabel, Matcher<String> explanationMatcher) {
        return new AllocationDecisionMatcher(Decision.Type.NO, equalTo(expectedLabel), explanationMatcher);
    }

    public static AllocationDecisionMatcher isNotPreferredDecision() {
        return new AllocationDecisionMatcher(
            Decision.Type.NOT_PREFERRED,
            any(String.class),
            anyOf(any(String.class), nullValue(String.class))
        );
    }

    public static AllocationDecisionMatcher isNotPreferredDecision(String expectedLabel) {
        return new AllocationDecisionMatcher(
            Decision.Type.NOT_PREFERRED,
            equalTo(expectedLabel),
            anyOf(any(String.class), nullValue(String.class))
        );
    }

    public static AllocationDecisionMatcher isNotPreferredDecisionWithExplanationMatching(
        String expectedLabel,
        Matcher<String> explanationMatcher
    ) {
        return new AllocationDecisionMatcher(Decision.Type.NOT_PREFERRED, equalTo(expectedLabel), explanationMatcher);
    }

    @Override
    public boolean matches(Object actual) {
        if (!(actual instanceof Decision decision)) {
            return false;
        }
        return decision.type() == expectedType && expectedLabel.matches(decision.label()) && explanationMatches(decision);
    }

    @Override
    public void describeMismatch(Object actual, Description mismatchDescription) {
        if (actual instanceof Decision decision) {
            switch (decision) {
                case Decision.Single single -> mismatchDescription.appendText("was a Decision.Single with type ")
                    .appendValue(single.type())
                    .appendText(" and label ")
                    .appendValue(single.label())
                    .appendText(" and explanation = {")
                    .appendValue(single.getExplanation())
                    .appendText("}");
                case Decision.Multi multi -> mismatchDescription.appendText("was a Decision.Multi with type ")
                    .appendValue(multi.type())
                    .appendText(" and label ")
                    .appendValue(multi.label())
                    .appendText(" and decisions = {")
                    .appendValue(multi.getDecisions().toString())
                    .appendText("}");
            }
        } else {
            mismatchDescription.appendText("was not a Decision");
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a Decision with type ")
            .appendValue(expectedType)
            .appendText(" and label ")
            .appendDescriptionOf(expectedLabel)
            .appendText(" and explanation = {")
            .appendDescriptionOf(explanationMatcher)
            .appendText("}");
    }

    /// {@link Decision.Multi#getExplanation} throws an {@link UnsupportedOperationException} so
    /// we traverse the decisions and assert that the effective one matches the specified pattern.
    ///
    /// This is only called for a multi-decision once we've already established the effective decision
    /// is the one specified by {@link #expectedLabel} matcher.
    private boolean explanationMatches(Decision decision) {
        return switch (decision) {
            case Decision.Single single -> explanationMatcher.matches(single.getExplanation());
            case Decision.Multi multi -> multi.decisions()
                .stream()
                .filter(d -> expectedLabel.matches(d.label()))
                .findFirst()
                .map(d -> explanationMatcher.matches(d.getExplanation()))
                .orElse(false);
        };
    }
}
