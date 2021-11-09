/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.jira;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.notification.jira.JiraAccount;
import org.elasticsearch.xpack.watcher.notification.jira.JiraService;
import org.junit.Before;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.jiraAction;
import static org.elasticsearch.xpack.watcher.notification.jira.JiraAccountTests.randomIssueDefaults;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JiraActionFactoryTests extends ESTestCase {

    private JiraService service;

    @Before
    public void init() throws Exception {
        service = mock(JiraService.class);
    }

    public void testParseAction() throws Exception {
        JiraAccount account = mock(JiraAccount.class);
        when(service.getAccount("_account1")).thenReturn(account);

        JiraAction action = jiraAction("_account1", randomIssueDefaults()).build();
        XContentBuilder jsonBuilder = jsonBuilder().value(action);
        XContentParser parser = createParser(jsonBuilder);
        parser.nextToken();

        JiraAction parsedAction = JiraAction.parse("_w1", "_a1", parser);
        assertThat(parsedAction, equalTo(action));
    }
}
