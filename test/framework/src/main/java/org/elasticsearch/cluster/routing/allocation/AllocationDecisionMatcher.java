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
import org.hamcrest.Matchers;

public class AllocationDecisionMatcher extends BaseMatcher<Decision> {

    private final Decision.Type expectedType;
    private final String expectedLabel;
    private final Matcher<String> explanationMatcher;

    public AllocationDecisionMatcher(Decision.Type expectedType, String expectedLabel, Matcher<String> explanationMatcher) {
        this.expectedType = expectedType;
        this.expectedLabel = expectedLabel;
        this.explanationMatcher = explanationMatcher;
    }

    public static AllocationDecisionMatcher isNoDecision(String expectedLabel) {
        return new AllocationDecisionMatcher(Decision.Type.NO, expectedLabel, Matchers.any(String.class));
    }

    public static AllocationDecisionMatcher isNoDecisionWithNoExplanation(String expectedLabel) {
        return new AllocationDecisionMatcher(Decision.Type.NO, expectedLabel, Matchers.nullValue(String.class));
    }

    public static AllocationDecisionMatcher isNoDecisionWithExplanationMatching(String expectedLabel, Matcher<String> explanationMatcher) {
        return new AllocationDecisionMatcher(Decision.Type.NO, expectedLabel, explanationMatcher);
    }

    public static AllocationDecisionMatcher isNotPreferredDecision(String expectedLabel) {
        return new AllocationDecisionMatcher(Decision.Type.NOT_PREFERRED, expectedLabel, Matchers.any(String.class));
    }

    @Override
    public boolean matches(Object actual) {
        if (!(actual instanceof Decision decision)) {
            return false;
        }
        return decision.type() == expectedType
            && expectedLabel.equals(decision.label())
            && explanationMatcher.matches(decision.getExplanation());
    }

    @Override
    public void describeMismatch(Object actual, Description mismatchDescription) {
        if (actual instanceof Decision decision) {
            mismatchDescription.appendText("was a Decision with type ")
                .appendValue(decision.type())
                .appendText(" and label ")
                .appendValue(decision.label())
                .appendText(" and explanation ")
                .appendValue(decision.getExplanation());
        } else {
            mismatchDescription.appendText("was not a Decision");
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a Decision with type ")
            .appendValue(expectedType)
            .appendText(" and label ")
            .appendValue(expectedLabel)
            .appendText(" and explanation ")
            .appendDescriptionOf(explanationMatcher);
    }
}
