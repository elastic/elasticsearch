/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.hipchat;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.notification.hipchat.HipChatAccount;
import org.elasticsearch.xpack.notification.hipchat.HipChatService;
import org.junit.Before;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.hipchatAction;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HipChatActionFactoryTests extends ESTestCase {
    private HipChatActionFactory factory;
    private HipChatService hipchatService;

    @Before
    public void init() throws Exception {
        hipchatService = mock(HipChatService.class);
        factory = new HipChatActionFactory(Settings.EMPTY, mock(TextTemplateEngine.class), hipchatService);
    }

    public void testParseAction() throws Exception {
        HipChatAccount account = mock(HipChatAccount.class);
        when(hipchatService.getAccount("_account1")).thenReturn(account);

        HipChatAction action = hipchatAction("_account1", "_body").build();
        XContentBuilder jsonBuilder = jsonBuilder().value(action);
        XContentParser parser = JsonXContent.jsonXContent.createParser(jsonBuilder.bytes());
        parser.nextToken();

        HipChatAction parsedAction = factory.parseAction("_w1", "_a1", parser);
        assertThat(parsedAction, is(action));

        verify(account, times(1)).validateParsedTemplate("_w1", "_a1", action.message);
    }

    public void testParseActionUnknownAccount() throws Exception {
        when(hipchatService.getAccount("_unknown")).thenReturn(null);

        HipChatAction action = hipchatAction("_unknown", "_body").build();
        XContentBuilder jsonBuilder = jsonBuilder().value(action);
        XContentParser parser = JsonXContent.jsonXContent.createParser(jsonBuilder.bytes());
        parser.nextToken();
        try {
            factory.parseAction("_w1", "_a1", parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [hipchat] action [_w1]. unknown hipchat account [_unknown]"));
        }
    }
}
