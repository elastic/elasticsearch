/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class DeprecationChecksTests extends ESTestCase {

    public void testFilterChecks() {
        DeprecationIssue issue = createRandomDeprecationIssue();
        int numChecksPassed = randomIntBetween(0, 5);
        int numChecksFailed = 10 - numChecksPassed;
        List<Supplier<DeprecationIssue>> checks = new ArrayList<>();
        for (int i = 0; i < numChecksFailed; i++) {
            checks.add(() -> issue);
        }
        for (int i = 0; i < numChecksPassed; i++) {
            checks.add(() -> null);
        }
        List<DeprecationIssue> filteredIssues = DeprecationInfoAction.filterChecks(checks, Supplier::get);
        assertThat(filteredIssues.size(), equalTo(numChecksFailed));
    }

    private static DeprecationIssue createRandomDeprecationIssue() {
        String details = randomBoolean() ? randomAlphaOfLength(10) : null;
        return new DeprecationIssue(
            randomFrom(DeprecationIssue.Level.values()),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            details,
            randomBoolean(),
            randomMap(1, 5, () -> Tuple.tuple(randomAlphaOfLength(4), randomAlphaOfLength(4)))
        );
    }
}
