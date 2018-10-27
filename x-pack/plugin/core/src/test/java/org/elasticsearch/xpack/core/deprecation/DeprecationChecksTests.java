/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.core.IsEqual.equalTo;

public class DeprecationChecksTests extends ESTestCase {

    public void testFilterChecks() throws IOException {
        DeprecationIssue issue = DeprecationIssueTests.createTestInstance();
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
}
