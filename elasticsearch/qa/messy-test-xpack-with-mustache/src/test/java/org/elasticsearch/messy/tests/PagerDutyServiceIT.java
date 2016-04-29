/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.messy.tests;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockMustacheScriptEngine;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.watcher.actions.pagerduty.PagerDutyAction;
import org.elasticsearch.xpack.notification.pagerduty.IncidentEvent;
import org.elasticsearch.xpack.notification.pagerduty.IncidentEventContext;
import org.elasticsearch.xpack.notification.pagerduty.PagerDutyAccount;
import org.elasticsearch.xpack.notification.pagerduty.PagerDutyService;
import org.elasticsearch.xpack.notification.pagerduty.SentEvent;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.watch.Payload;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.watcher.actions.ActionBuilders.pagerDutyAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
@Network
public class PagerDutyServiceIT extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean timeWarped() {
        return true;
    }

    @Override
    protected boolean enableShield() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Collection<Class<? extends Plugin>> mockPlugins = super.getMockPlugins();
        mockPlugins.remove(MockMustacheScriptEngine.TestPlugin.class);
        return mockPlugins;
    }

    @Override
    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = super.pluginTypes();
        types.add(MustachePlugin.class);
        return types;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("xpack.notification.pagerduty.account.test_account.service_api_key", "fc082467005d4072a914e0bb041882d0")
                .build();
    }

    public void testSendTriggerEvent() throws Exception {
        PagerDutyService service = getInstanceFromMaster(PagerDutyService.class);

        IncidentEvent event = new IncidentEvent("#testIncidentEvent()", null, null, "PagerDutyServiceTests", "_client_url", "_account",
                true, new IncidentEventContext[] {
                IncidentEventContext.link("_href", "_text"),
                IncidentEventContext.image("_src", "_href", "_alt")
        });

        Payload payload = new Payload.Simple("_key", "_val");

        PagerDutyAccount account = service.getAccount("test_account");
        assertThat(account, notNullValue());
        SentEvent sentEvent = account.send(event, payload);
        assertThat(sentEvent, notNullValue());
        assertThat(sentEvent.successful(), is(true));
        assertThat(sentEvent.getRequest(), notNullValue());
        assertThat(sentEvent.getResponse(), notNullValue());
        assertThat(sentEvent.getResponse().status(), lessThan(300));
    }

    public void testWatchWithPagerDutyAction() throws Exception {
        String account = "test_account";
        PagerDutyAction.Builder actionBuilder = pagerDutyAction(IncidentEvent
                .templateBuilder("pager duty integration test `{{ctx.payload.ref}}`").setAccount(account));

        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("1").setSource(watchBuilder()
                .trigger(schedule(interval("10m")))
                .input(simpleInput("ref", "testWatchWithPagerDutyAction()"))
                .condition(alwaysCondition())
                .addAction("pd", actionBuilder))
                .execute().get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().scheduler().trigger("1");
        flush();
        refresh();

        assertWatchWithMinimumPerformedActionsCount("1", 1L, false);
        SearchResponse response = searchHistory(searchSource().query(boolQuery()
                .must(termQuery("result.actions.id", "pd"))
                .must(termQuery("result.actions.type", "pagerduty"))
                .must(termQuery("result.actions.status", "success"))
                .must(termQuery("result.actions.pagerduty.sent_event.event.account", account))));

        assertThat(response, notNullValue());
        assertHitCount(response, 1L);
    }
}
