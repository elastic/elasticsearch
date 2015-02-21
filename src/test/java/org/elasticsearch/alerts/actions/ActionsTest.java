/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;


import org.elasticsearch.alerts.AbstractAlertingTests;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.actions.index.IndexAction;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.search.ScriptSearchCondition;
import org.elasticsearch.alerts.scheduler.schedule.CronSchedule;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.transform.SearchTransform;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequest;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.alerts.transport.actions.get.GetAlertRequest;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.alerts.transport.actions.put.PutAlertRequest;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.ScriptService;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 */
public class ActionsTest extends AbstractAlertingTests {

    @Test
    public void testAlertActions() throws Exception {
        //TODO: Consider deleting this test or making it do something useful
        createIndex("my-index");

        ensureGreen("my-index");

        client().preparePutIndexedScript()
                .setScriptLang("mustache")
                .setId("query")
                .setSource(jsonBuilder().startObject().startObject("template").startObject("match_all").endObject().endObject().endObject())
                .get();

        ensureAlertingStarted();

        final Action alertAction = new IndexAction(logger, ClientProxy.of(client()), "testindex", "testtype");
        final List<Action> actionList = new ArrayList<>();
        actionList.add(alertAction);

        Condition alertCondition = new ScriptSearchCondition(logger, scriptService(),
                ClientProxy.of(client()), createConditionSearchRequest(), "return true", ScriptService.ScriptType.INLINE, "groovy");


        Alert alert = new Alert(
                "my-first-alert",
                new CronSchedule("0/5 * * * * ? *"),
                alertCondition,
                new SearchTransform(logger, scriptService(), ClientProxy.of(client()), createConditionSearchRequest()),
                new TimeValue(0),
                new Actions(actionList),
                null,
                new Alert.Status());

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

        PutAlertRequest alertRequest = alertClient().preparePutAlert().setAlertName("my-first-alert").setAlertSource(jsonBuilder.bytes()).request();
        PutAlertResponse alertsResponse = alertClient().putAlert(alertRequest).actionGet();
        assertNotNull(alertsResponse.indexResponse());
        assertTrue(alertsResponse.indexResponse().isCreated());

        GetAlertRequest getAlertRequest = new GetAlertRequest(alert.name());
        GetAlertResponse getAlertResponse = alertClient().getAlert(getAlertRequest).actionGet();
        assertTrue(getAlertResponse.getResponse().isExists());
        assertEquals(((Map<String,Object>)getAlertResponse.getResponse().getSourceAsMap().get("schedule")).get("cron").toString(), "0/5 * * * * ? *");

        DeleteAlertRequest deleteAlertRequest = new DeleteAlertRequest(alert.name());
        DeleteAlertResponse deleteAlertResponse = alertClient().deleteAlert(deleteAlertRequest).actionGet();
        assertNotNull(deleteAlertResponse.deleteResponse());
        assertTrue(deleteAlertResponse.deleteResponse().isFound());

        getAlertResponse = alertClient().getAlert(getAlertRequest).actionGet();
        assertFalse(getAlertResponse.getResponse().isExists());

    }

}
