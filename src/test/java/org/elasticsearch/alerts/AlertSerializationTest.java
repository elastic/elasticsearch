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
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AlertSerializationTest extends ElasticsearchIntegrationTest {

    @Test
    public void testAlertSerialization() throws Exception {

        SearchRequest request = new SearchRequest();
        request.indices("my-index");
        List<AlertAction> actions = new ArrayList<>();
        actions.add(new EmailAlertAction("message", "foo@bar.com"));
        Alert alert = new Alert("test-serialization",
                request,
                new ScriptedTrigger("return true", ScriptService.ScriptType.INLINE, "groovy"),
                actions,
                "0/5 * * * * ? *",
                new DateTime(),
                0,
                new TimeValue(0),
                AlertAckState.NOT_TRIGGERED);


        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

        final AlertsStore alertsStore =
                internalCluster().getInstance(AlertsStore.class, internalCluster().getMasterName());

        Alert parsedAlert = alertsStore.parseAlert("test-serialization", jsonBuilder.bytes());
        assertEquals(parsedAlert.version(), alert.version());
        assertEquals(parsedAlert.actions(), alert.actions());
        assertEquals(parsedAlert.lastActionFire().getMillis(), alert.lastActionFire().getMillis());
        assertEquals(parsedAlert.schedule(), alert.schedule());
        assertEquals(parsedAlert.getSearchRequest().source(), alert.getSearchRequest().source());
        assertEquals(parsedAlert.trigger(), alert.trigger());
        assertEquals(parsedAlert.getThrottlePeriod(), alert.getThrottlePeriod());
        if (parsedAlert.getTimeLastActionExecuted() == null) {
            assertNull(alert.getTimeLastActionExecuted());
        }
        assertEquals(parsedAlert.getAckState(), parsedAlert.getAckState());
    }




}
