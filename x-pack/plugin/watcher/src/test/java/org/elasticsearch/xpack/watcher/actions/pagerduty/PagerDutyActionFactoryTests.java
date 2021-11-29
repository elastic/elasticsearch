/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.pagerduty;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.pagerduty.PagerDutyAccount;
import org.elasticsearch.xpack.watcher.notification.pagerduty.PagerDutyService;
import org.junit.Before;

import java.util.HashSet;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.triggerPagerDutyAction;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PagerDutyActionFactoryTests extends ESTestCase {

    private PagerDutyActionFactory factory;
    private PagerDutyService service;

    @Before
    public void init() throws Exception {
        service = mock(PagerDutyService.class);
        factory = new PagerDutyActionFactory(mock(TextTemplateEngine.class), service);
    }

    public void testParseAction() throws Exception {

        PagerDutyAccount account = mock(PagerDutyAccount.class);
        when(service.getAccount("_account1")).thenReturn(account);

        PagerDutyAction action = triggerPagerDutyAction("_account1", "_description").build();
        XContentBuilder jsonBuilder = jsonBuilder().value(action);
        XContentParser parser = createParser(jsonBuilder);
        parser.nextToken();

        PagerDutyAction parsedAction = PagerDutyAction.parse("_w1", "_a1", parser);
        assertThat(parsedAction, is(action));
    }

    public void testParseActionUnknownAccount() throws Exception {
        factory = new PagerDutyActionFactory(
            mock(TextTemplateEngine.class),
            new PagerDutyService(Settings.EMPTY, null, new ClusterSettings(Settings.EMPTY, new HashSet<>(PagerDutyService.getSettings())))
        );
        PagerDutyAction action = triggerPagerDutyAction("_unknown", "_body").build();
        XContentBuilder jsonBuilder = jsonBuilder().value(action);
        XContentParser parser = createParser(jsonBuilder);
        parser.nextToken();
        expectThrows(IllegalArgumentException.class, () -> factory.parseExecutable("_w1", "_a1", parser));
    }
}
