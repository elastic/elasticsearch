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

public class AllocationDecisionMatcher extends BaseMatcher<Decision> {

    private final Decision.Type expectedType;

    public AllocationDecisionMatcher(Decision.Type expectedType) {
        this.expectedType = expectedType;
    }

    public static AllocationDecisionMatcher isNoDecision() {
        return new AllocationDecisionMatcher(Decision.Type.NO);
    }

    @Override
    public boolean matches(Object actual) {
        if (!(actual instanceof Decision)) {
            return false;
        }
        Decision decision = (Decision) actual;
        return decision.type() == expectedType;
    }

    @Override
    public void describeMismatch(Object actual, Description mismatchDescription) {
        if (actual instanceof Decision) {
            Decision decision = (Decision) actual;
            mismatchDescription.appendText("was a Decision with type ").appendValue(decision.type());
        } else {
            mismatchDescription.appendText("was not a Decision");
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a Decision with type ").appendValue(expectedType);
    }
}
