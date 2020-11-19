/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportDeprecationInfoActionTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool("TransportDeprecationInfoActionTests");
    }

    @After
    public void stopThreadPool() throws InterruptedException {
        terminate(threadPool);
    }

    public void testPluginSettingIssues() {
        DeprecationChecker.Components components = mock(DeprecationChecker.Components.class);
        when(components.settings()).thenReturn(Settings.EMPTY);
        PlainActionFuture<Map<String, List<DeprecationIssue>>> future = new PlainActionFuture<>();
        TransportDeprecationInfoAction.pluginSettingIssues(Arrays.asList(
            new NamedChecker("foo", Collections.emptyList(), false),
            new NamedChecker("bar",
                Collections.singletonList(new DeprecationIssue(DeprecationIssue.Level.WARNING, "bar msg", "", null)),
                false)),
            threadPool.generic(),
            components,
            future
            );
        Map<String, List<DeprecationIssue>> issueMap = future.actionGet();
        assertThat(issueMap.size(), equalTo(2));
        assertThat(issueMap.get("foo"), is(empty()));
        assertThat(issueMap.get("bar").get(0).getMessage(), equalTo("bar msg"));
    }

    public void testPluginSettingIssuesWithFailures() {
        DeprecationChecker.Components components = mock(DeprecationChecker.Components.class);
        when(components.settings()).thenReturn(Settings.EMPTY);
        PlainActionFuture<Map<String, List<DeprecationIssue>>> future = new PlainActionFuture<>();
        TransportDeprecationInfoAction.pluginSettingIssues(Arrays.asList(
            new NamedChecker("foo", Collections.emptyList(), false),
            new NamedChecker("bar",
                Collections.singletonList(new DeprecationIssue(DeprecationIssue.Level.WARNING, "bar msg", "", null)),
                true)),
            threadPool.generic(),
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
        public void check(Components components, ActionListener<List<DeprecationIssue>> deprecationIssueListener) {
            if (shouldFail) {
                deprecationIssueListener.onFailure(new Exception("boom"));
                return;
            }
            deprecationIssueListener.onResponse(issues);
        }

        @Override
        public String getName() {
            return name;
        }
    }

}
