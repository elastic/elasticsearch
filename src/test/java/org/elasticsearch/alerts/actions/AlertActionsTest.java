/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numClientNodes = 0, transportClientRatio = 0, numDataNodes = 1)
public class AlertActionsTest extends ElasticsearchIntegrationTest {

    private static final FormatDateTimeFormatter formatter = DateFieldMapper.Defaults.DATE_TIME_FORMATTER;

    @Test
    public void testAlertActionParser() throws Exception {
        DateTime fireTime = new DateTime(DateTimeZone.UTC);
        DateTime scheduledFireTime = new DateTime(DateTimeZone.UTC);
        Map<String, Object> triggerMap = new HashMap<>();
        triggerMap.put("numberOfEvents", ">1");
        Map<String,Object> actionMap = new HashMap<>();
        Map<String,Object> emailParamMap = new HashMap<>();
        List<String> addresses = new ArrayList<>();
        addresses.add("foo@bar.com");
        emailParamMap.put("addresses", addresses);
        actionMap.put("email", emailParamMap);

        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.field(AlertActionManager.ALERT_NAME_FIELD, "testName");
        builder.field(AlertActionManager.TRIGGERED_FIELD, true);
        builder.field(AlertActionManager.FIRE_TIME_FIELD, formatter.printer().print(fireTime));
        builder.field(AlertActionManager.SCHEDULED_FIRE_TIME_FIELD, formatter.printer().print(scheduledFireTime));
        builder.field(AlertActionManager.TRIGGER_FIELD, triggerMap);
        BytesStreamOutput out = new BytesStreamOutput();
        SearchRequest searchRequest = new SearchRequest("test123");
        searchRequest.writeTo(out);
        builder.field(AlertActionManager.REQUEST, out.bytes());
        SearchResponse searchResponse = new SearchResponse(
                new InternalSearchResponse(new InternalSearchHits(new InternalSearchHit[0], 10, 0), null, null, null, false, false),
                null, 1, 1, 0, new ShardSearchFailure[0]
        );
        out = new BytesStreamOutput();
        searchResponse.writeTo(out);
        builder.field(AlertActionManager.RESPONSE, out.bytes());
        builder.field(AlertActionManager.ACTIONS_FIELD, actionMap);
        builder.field(AlertActionState.FIELD_NAME, AlertActionState.ACTION_NEEDED.toString());
        builder.endObject();
        AlertActionRegistry alertActionRegistry = internalCluster().getInstance(AlertActionRegistry.class, internalCluster().getMasterName());
        AlertActionEntry actionEntry = AlertActionManager.parseHistory("foobar", builder.bytes(), 0, alertActionRegistry);

        assertEquals(actionEntry.getVersion(), 0);
        assertEquals(actionEntry.getAlertName(), "testName");
        assertEquals(actionEntry.isTriggered(), true);
        assertEquals(actionEntry.getScheduledTime(), scheduledFireTime);
        assertEquals(actionEntry.getFireTime(), fireTime);
        assertEquals(actionEntry.getEntryState(), AlertActionState.ACTION_NEEDED);
        assertEquals(actionEntry.getSearchResponse().getHits().getTotalHits(), 10);
        assertEquals(actionEntry.getTrigger(),
                new AlertTrigger(AlertTrigger.SimpleTrigger.GREATER_THAN, AlertTrigger.TriggerType.NUMBER_OF_EVENTS, 1));

    }
}
