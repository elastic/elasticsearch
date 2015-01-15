/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.alerts.AbstractAlertingTests;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertAckState;
import org.elasticsearch.alerts.triggers.ScriptedTrigger;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

/**
*/
public class EmailTemplateTest extends AbstractAlertingTests {

    public void testEmailTemplateRender() throws IOException {
        createIndex("my-trigger-index");

        SearchRequest triggerRequest = createTriggerSearchRequest("my-trigger-index").source(searchSource().query(matchAllQuery()));

        List<AlertAction> actions = new ArrayList<>();


        Alert alert = new Alert("test-email-template",
                triggerRequest,
                new ScriptedTrigger("return true", ScriptService.ScriptType.INLINE, "groovy"),
                actions,
                "0/5 * * * * ? *",
                new DateTime(),
                0,
                new TimeValue(0),
                AlertAckState.NOT_TRIGGERED);

        SearchResponse searchResponse = client().prepareSearch("my-trigger-index").get();
        XContentBuilder responseBuilder = jsonBuilder().startObject().value(searchResponse).endObject();
        Map<String, Object> responseMap = XContentHelper.convertToMap(responseBuilder.bytes(), false).v2();

        TriggerResult result = new TriggerResult(true, triggerRequest, responseMap, alert.getTrigger());

        String template = "{{alert_name}} triggered with {{response.hits.total}} hits";


        ScriptService scriptService = internalTestCluster().getInstance(ScriptService.class);
        String parsedTemplate = SmtpAlertActionFactory.renderTemplate(template, alert, result, scriptService);
        assertEquals("test-email-template triggered with 0 hits", parsedTemplate);

    }

}
