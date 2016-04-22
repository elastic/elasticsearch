/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.slack;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.notification.slack.SlackAccount;
import org.elasticsearch.xpack.notification.slack.SlackService;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;
import org.junit.Before;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.actions.ActionBuilders.slackAction;
import static org.elasticsearch.xpack.notification.slack.message.SlackMessageTests.createRandomTemplate;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class SlackActionFactoryTests extends ESTestCase {
    private SlackActionFactory factory;
    private SlackService service;

    @Before
    public void init() throws Exception {
        service = mock(SlackService.class);
        factory = new SlackActionFactory(Settings.EMPTY, mock(TextTemplateEngine.class), service);
    }

    public void testParseAction() throws Exception {
        SlackAccount account = mock(SlackAccount.class);
        when(service.getAccount("_account1")).thenReturn(account);

        SlackAction action = slackAction("_account1", createRandomTemplate()).build();
        XContentBuilder jsonBuilder = jsonBuilder().value(action);
        XContentParser parser = JsonXContent.jsonXContent.createParser(jsonBuilder.bytes());
        parser.nextToken();

        SlackAction parsedAction = factory.parseAction("_w1", "_a1", parser);
        assertThat(parsedAction, is(action));
    }

    public void testParseActionUnknownAccount() throws Exception {
        when(service.getAccount("_unknown")).thenReturn(null);

        SlackAction action = slackAction("_unknown", createRandomTemplate()).build();
        XContentBuilder jsonBuilder = jsonBuilder().value(action);
        XContentParser parser = JsonXContent.jsonXContent.createParser(jsonBuilder.bytes());
        parser.nextToken();
        try {
            factory.parseAction("_w1", "_a1", parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [slack] action [_w1]. unknown slack account [_unknown]"));
        }
    }
}
