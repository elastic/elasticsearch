/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransportDeprecationInfoActionTests extends ESTestCase {

    public void testPluginSettingIssues() {
        DeprecationChecker.Components components = new DeprecationChecker.Components(null, Settings.EMPTY, null);
        PlainActionFuture<Map<String, List<DeprecationIssue>>> future = new PlainActionFuture<>();
        TransportDeprecationInfoAction.pluginSettingIssues(Arrays.asList(
            new NamedChecker("foo", Collections.emptyList(), false),
            new NamedChecker("bar",
                List.of(new DeprecationIssue(DeprecationIssue.Level.WARNING, "bar msg", "", "details", Map.of("key", "value"))), false)),
            components,
            future
            );
        Map<String, List<DeprecationIssue>> issueMap = future.actionGet();
        assertThat(issueMap.size(), equalTo(2));
        assertThat(issueMap.get("foo"), is(empty()));
        assertThat(issueMap.get("bar").get(0).getMessage(), equalTo("bar msg"));
        assertThat(issueMap.get("bar").get(0).getDetails(), equalTo("details"));
        assertThat(issueMap.get("bar").get(0).getMeta(), equalTo(Map.of("key", "value")));
    }

    public void testPluginSettingIssuesWithFailures() {
        DeprecationChecker.Components components = new DeprecationChecker.Components(null, Settings.EMPTY, null);
        PlainActionFuture<Map<String, List<DeprecationIssue>>> future = new PlainActionFuture<>();
        TransportDeprecationInfoAction.pluginSettingIssues(Arrays.asList(
            new NamedChecker("foo", Collections.emptyList(), false),
            new NamedChecker("bar",
                Collections.singletonList(new DeprecationIssue(DeprecationIssue.Level.WARNING, "bar msg", "", null, null)),
                true)),
            components,
            future
        );
        Exception exception = expectThrows(Exception.class, future::actionGet);
        assertThat(exception.getCause().getMessage(), containsString("boom"));
    }

    private static class NamedChecker implements DeprecationChecker {

        private final String name;
        private final List<DeprecationIssue> issues;
        private final boolean shouldFail;

        NamedChecker(String name, List<DeprecationIssue> issues, boolean shouldFail) {
            this.name = name;
            this.issues = issues;
            this.shouldFail = shouldFail;
        }

        @Override
        public boolean enabled(Settings settings) {
            return true;
        }

        @Override
        public void check(DeprecationChecker.Components components, ActionListener<CheckResult> deprecationIssueListener) {
            if (shouldFail) {
                deprecationIssueListener.onFailure(new Exception("boom"));
                return;
            }
            deprecationIssueListener.onResponse(new CheckResult(name, issues));
        }

        @Override
        public String getName() {
            return name;
        }
    }

}
