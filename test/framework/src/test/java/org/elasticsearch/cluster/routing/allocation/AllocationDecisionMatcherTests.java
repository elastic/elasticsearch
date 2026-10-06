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
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.StringDescription;

import java.util.Arrays;

import static org.elasticsearch.cluster.routing.allocation.AllocationDecisionMatcher.isNoDecision;
import static org.elasticsearch.cluster.routing.allocation.AllocationDecisionMatcher.isNoDecisionWithExplanationMatching;
import static org.elasticsearch.cluster.routing.allocation.AllocationDecisionMatcher.isNoDecisionWithNoExplanation;
import static org.elasticsearch.cluster.routing.allocation.AllocationDecisionMatcher.isNotPreferredDecision;
import static org.elasticsearch.cluster.routing.allocation.AllocationDecisionMatcher.isNotPreferredDecisionWithExplanationMatching;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AllocationDecisionMatcherTests extends ESTestCase {

    public void testNonDecisionDoesNotMatch() {
        assertThat(isNoDecision().matches("not a decision"), equalTo(false));
        assertThat(isNoDecision().matches(null), equalTo(false));
    }

    public void testTypeMustMatch() {
        final var no = new Decision.Single(Decision.Type.NO, "label", "explanation");
        final var notPreferred = new Decision.Single(Decision.Type.NOT_PREFERRED, "label", "explanation");
        assertThat(isNoDecision().matches(no), equalTo(true));
        assertThat(isNoDecision().matches(notPreferred), equalTo(false));
        assertThat(isNotPreferredDecision().matches(notPreferred), equalTo(true));
        assertThat(isNotPreferredDecision().matches(no), equalTo(false));
        assertThat(isNoDecision().matches(Decision.YES), equalTo(false));
    }

    public void testLabelMustMatchWhenSpecified() {
        final var label = randomAlphaOfLength(8);
        final var no = new Decision.Single(Decision.Type.NO, label, null);
        final var notPreferred = new Decision.Single(Decision.Type.NOT_PREFERRED, label, null);
        assertThat(isNoDecision(label).matches(no), equalTo(true));
        assertThat(isNoDecision(label + "x").matches(no), equalTo(false));
        assertThat(isNotPreferredDecision(label).matches(notPreferred), equalTo(true));
        assertThat(isNotPreferredDecision(label + "x").matches(notPreferred), equalTo(false));
    }

    public void testLabelAgnosticMatchersAcceptNullLabel() {
        final var no = new Decision.Single(Decision.Type.NO, null, null);
        final var notPreferred = new Decision.Single(Decision.Type.NOT_PREFERRED, null, null);
        assertThat(isNoDecision().matches(no), equalTo(true));
        assertThat(isNoDecision().matches(new Decision.Multi().add(no)), equalTo(true));
        assertThat(isNotPreferredDecision().matches(notPreferred), equalTo(true));
        assertThat(isNotPreferredDecision().matches(new Decision.Multi().add(notPreferred)), equalTo(true));
    }

    public void testLabelSpecificMatchersRejectNullLabel() {
        assertThat(isNoDecision("label").matches(new Decision.Single(Decision.Type.NO, null, null)), equalTo(false));
        assertThat(isNotPreferredDecision("label").matches(new Decision.Single(Decision.Type.NOT_PREFERRED, null, null)), equalTo(false));
    }

    public void testExplanationMatching() {
        final var label = randomAlphaOfLength(8);
        final var withExplanation = new Decision.Single(Decision.Type.NO, label, "some explanation");
        final var withoutExplanation = new Decision.Single(Decision.Type.NO, label, null);

        assertThat(isNoDecision(label).matches(withExplanation), equalTo(true));
        assertThat(isNoDecision(label).matches(withoutExplanation), equalTo(true));

        assertThat(isNoDecisionWithNoExplanation(label).matches(withoutExplanation), equalTo(true));
        assertThat(isNoDecisionWithNoExplanation(label).matches(withExplanation), equalTo(false));

        assertThat(isNoDecisionWithExplanationMatching(label, containsString("some")).matches(withExplanation), equalTo(true));
        assertThat(isNoDecisionWithExplanationMatching(label, containsString("other")).matches(withExplanation), equalTo(false));

        final var notPreferred = new Decision.Single(Decision.Type.NOT_PREFERRED, label, "some explanation");
        assertThat(isNotPreferredDecisionWithExplanationMatching(label, containsString("some")).matches(notPreferred), equalTo(true));
        assertThat(isNotPreferredDecisionWithExplanationMatching(label, containsString("other")).matches(notPreferred), equalTo(false));
    }

    public void testMultiDecisionMatchesOnEffectiveDecision() {
        final var multi = new Decision.Multi().add(Decision.YES)
            .add(new Decision.Single(Decision.Type.NOT_PREFERRED, "not-preferred-label", "not preferred explanation"))
            .add(new Decision.Single(Decision.Type.NO, "no-label", "no explanation"));

        assertThat(isNoDecision().matches(multi), equalTo(true));
        assertThat(isNoDecision("no-label").matches(multi), equalTo(true));
        assertThat(isNoDecisionWithExplanationMatching("no-label", containsString("no expl")).matches(multi), equalTo(true));
        assertThat(isNoDecisionWithExplanationMatching("no-label", containsString("not preferred")).matches(multi), equalTo(false));

        // NOT_PREFERRED is present, but is not the effective decision
        assertThat(isNotPreferredDecision().matches(multi), equalTo(false));
        assertThat(isNoDecision("not-preferred-label").matches(multi), equalTo(false));
    }

    public void testEmptyMultiDecisionDoesNotMatch() {
        assertThat(isNoDecision().matches(new Decision.Multi()), equalTo(false));
    }

    public void testDescribeMismatch() {
        assertThat(describeMismatch("not a decision"), equalTo("was not a Decision"));
        assertThat(
            describeMismatch(new Decision.Single(Decision.Type.YES, "label", "explanation")),
            allOf("Decision.Single", "type <YES>", "label \"label\"", "explanation")
        );
        assertThat(
            describeMismatch(new Decision.Multi().add(new Decision.Single(Decision.Type.THROTTLE, "label", "explanation"))),
            allOf("Decision.Multi", "type <THROTTLE>", "label \"label\"", "decisions")
        );
    }

    public void testDescribeTo() {
        final var description = StringDescription.toString(isNoDecision("my-label"));
        assertThat(description, containsString("type <NO>"));
        assertThat(description, containsString("my-label"));
        assertThat(description, not(containsString("NOT_PREFERRED")));
    }

    private static String describeMismatch(Object actual) {
        final var description = new StringDescription();
        isNoDecision().describeMismatch(actual, description);
        return description.toString();
    }

    private static Matcher<String> allOf(String... fragments) {
        return Matchers.allOf(Arrays.stream(fragments).<Matcher<? super String>>map(Matchers::containsString).toList());
    }
}
