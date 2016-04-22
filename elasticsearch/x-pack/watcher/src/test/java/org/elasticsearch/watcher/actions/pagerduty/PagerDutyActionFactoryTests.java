/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.pagerduty;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.notification.pagerduty.PagerDutyAccount;
import org.elasticsearch.xpack.notification.pagerduty.PagerDutyService;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;
import org.junit.Before;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.actions.ActionBuilders.triggerPagerDutyAction;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class PagerDutyActionFactoryTests extends ESTestCase {

    private PagerDutyActionFactory factory;
    private PagerDutyService service;

    @Before
    public void init() throws Exception {
        service = mock(PagerDutyService.class);
        factory = new PagerDutyActionFactory(Settings.EMPTY, mock(TextTemplateEngine.class), service);
    }

    public void testParseAction() throws Exception {

        PagerDutyAccount account = mock(PagerDutyAccount.class);
        when(service.getAccount("_account1")).thenReturn(account);

        PagerDutyAction action = triggerPagerDutyAction("_account1", "_description").build();
        XContentBuilder jsonBuilder = jsonBuilder().value(action);
        XContentParser parser = JsonXContent.jsonXContent.createParser(jsonBuilder.bytes());
        parser.nextToken();

        PagerDutyAction parsedAction = factory.parseAction("_w1", "_a1", parser);
        assertThat(parsedAction, is(action));
    }

    public void testParseActionUnknownAccount() throws Exception {
        try {
            when(service.getAccount("_unknown")).thenReturn(null);

            PagerDutyAction action = triggerPagerDutyAction("_unknown", "_body").build();
            XContentBuilder jsonBuilder = jsonBuilder().value(action);
            XContentParser parser = JsonXContent.jsonXContent.createParser(jsonBuilder.bytes());
            parser.nextToken();
            factory.parseAction("_w1", "_a1", parser);
            fail("Expected ElasticsearchParseException due to unknown account");
        } catch (ElasticsearchParseException e) {}
    }
}
