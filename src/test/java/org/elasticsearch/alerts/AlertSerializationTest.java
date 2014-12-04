/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.actions.EmailAlertAction;
import org.elasticsearch.alerts.triggers.ScriptedTrigger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.ScriptService;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

public class AlertSerializationTest extends AbstractAlertingTests {

    @Test
    public void testAlertSerialization() throws Exception {

        SearchRequest triggerRequest = createTriggerSearchRequest("my-trigger-index").source(searchSource().query(matchAllQuery()));
        SearchRequest payloadRequest = createTriggerSearchRequest("my-payload-index").source(searchSource().query(matchAllQuery()));

        List<AlertAction> actions = new ArrayList<>();
        actions.add(new EmailAlertAction("message", "foo@bar.com"));
        Alert alert = new Alert("test-serialization",
                triggerRequest,
                new ScriptedTrigger("return true", ScriptService.ScriptType.INLINE, "groovy"),
                actions,
                "0/5 * * * * ? *",
                new DateTime(),
                0,
                new TimeValue(0),
                AlertAckState.NOT_TRIGGERED);

        alert.setPayloadSearchRequest(payloadRequest);
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("list", "baz");
        alert.setMetadata(metadata);

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

        final AlertsStore alertsStore =
                internalTestCluster().getInstance(AlertsStore.class, internalTestCluster().getMasterName());

        Alert parsedAlert = alertsStore.parseAlert("test-serialization", jsonBuilder.bytes());

        assertEquals(parsedAlert.getVersion(), alert.getVersion());
        assertEquals(parsedAlert.getActions(), alert.getActions());
        assertEquals(parsedAlert.getLastExecuteTime().getMillis(), alert.getLastExecuteTime().getMillis());
        assertEquals(parsedAlert.getSchedule(), alert.getSchedule());
        assertEquals(parsedAlert.getTriggerSearchRequest().indices()[0], "my-trigger-index");
        assertEquals(parsedAlert.getPayloadSearchRequest().indices()[0], "my-payload-index");
        assertEquals(parsedAlert.getTrigger(), alert.getTrigger());
        assertEquals(parsedAlert.getThrottlePeriod(), alert.getThrottlePeriod());
        if (parsedAlert.getTimeLastActionExecuted() == null) {
            assertNull(alert.getTimeLastActionExecuted());
        }
        assertEquals(parsedAlert.getAckState(), alert.getAckState());
        assertEquals(parsedAlert.getMetadata(), alert.getMetadata());
    }

}
