/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.alerts.BasicAlertingTest;
import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numClientNodes = 0, transportClientRatio = 0, numDataNodes = 1)
public class AlertActionsTest extends ElasticsearchIntegrationTest {

    @Test
    public void testAlertActionParser(){
        DateTime fireTime = new DateTime();
        DateTime scheduledFireTime = new DateTime();
        Map<String, Object> triggerMap = new HashMap<>();
        triggerMap.put("numberOfEvents", ">1");
        Map<String,Object> actionMap = new HashMap<>();
        Map<String,Object> emailParamMap = new HashMap<>();
        List<String> addresses = new ArrayList<>();
        addresses.add("foo@bar.com");
        emailParamMap.put("addresses", addresses);
        actionMap.put("email", emailParamMap);

        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put(AlertActionManager.ALERT_NAME_FIELD, "testName");
        fieldMap.put(AlertActionManager.TRIGGERED_FIELD, true);
        fieldMap.put(AlertActionManager.FIRE_TIME_FIELD, fireTime.toDateTimeISO().toString());
        fieldMap.put(AlertActionManager.SCHEDULED_FIRE_TIME_FIELD, scheduledFireTime.toDateTimeISO().toString());
        fieldMap.put(AlertActionManager.TRIGGER_FIELD, triggerMap);
        fieldMap.put(AlertActionManager.QUERY_RAN_FIELD, "foobar");
        fieldMap.put(AlertActionManager.NUMBER_OF_RESULTS_FIELD,10);
        fieldMap.put(AlertActionManager.ACTIONS_FIELD, actionMap);
        fieldMap.put(AlertActionState.FIELD_NAME, AlertActionState.ACTION_NEEDED.toString());
        AlertActionRegistry alertActionRegistry = internalCluster().getInstance(AlertActionRegistry.class, internalCluster().getMasterName());
        AlertActionEntry actionEntry = AlertActionManager.parseHistory("foobar", fieldMap, 0, alertActionRegistry, logger);

        assertEquals(actionEntry.getVersion(), 0);
        assertEquals(actionEntry.getAlertName(), "testName");
        assertEquals(actionEntry.isTriggered(), true);
        assertEquals(actionEntry.getScheduledTime(), scheduledFireTime);
        assertEquals(actionEntry.getFireTime(), fireTime);
        assertEquals(actionEntry.getEntryState(), AlertActionState.ACTION_NEEDED);
        assertEquals(actionEntry.getNumberOfResults(), 10);
        assertEquals(actionEntry.getTrigger(),
                new AlertTrigger(AlertTrigger.SimpleTrigger.GREATER_THAN, AlertTrigger.TriggerType.NUMBER_OF_EVENTS, 1));

    }
}
