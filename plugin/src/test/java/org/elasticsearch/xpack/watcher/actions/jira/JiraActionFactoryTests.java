/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.jira;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.notification.jira.JiraAccount;
import org.elasticsearch.xpack.notification.jira.JiraService;
import org.junit.Before;

import static java.util.Collections.singleton;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.notification.jira.JiraAccountTests.randomIssueDefaults;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.jiraAction;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JiraActionFactoryTests extends ESTestCase {

    private JiraActionFactory factory;
    private JiraService service;

    @Before
    public void init() throws Exception {
        service = mock(JiraService.class);
        factory = new JiraActionFactory(Settings.EMPTY, mock(TextTemplateEngine.class), service);
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

    public void testParseActionUnknownAccount() throws Exception {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, singleton(JiraService.JIRA_ACCOUNT_SETTING));
        JiraService service = new JiraService(Settings.EMPTY, null, clusterSettings);
        factory = new JiraActionFactory(Settings.EMPTY, mock(TextTemplateEngine.class), service);

        JiraAction action = jiraAction("_unknown", randomIssueDefaults()).build();
        XContentBuilder jsonBuilder = jsonBuilder().value(action);
        XContentParser parser = createParser(jsonBuilder);
        parser.nextToken();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> factory.parseExecutable("_w1", "_a1", parser));
        assertThat(e.getMessage(), containsString("no account found for name: [_unknown]"));
    }
}
