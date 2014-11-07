/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.triggers.ScriptedTrigger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;

public class TestAlertSerialization extends ElasticsearchIntegrationTest {

    @Test
    public void testAlertSerialization() throws Exception {
        Alert alert = new Alert("test-serialization",
                new SearchRequest(),
                new ScriptedTrigger("return true", null, null),
                new ArrayList<AlertAction>(), "0/5 * * * * ? *",
                new DateTime(),
                0,
                false);

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

        final AlertsStore alertsStore =
                internalCluster().getInstance(AlertsStore.class, internalCluster().getMasterName());

        Alert parsedAlert = alertsStore.parseAlert("test-serialization", jsonBuilder.bytes());
        assertEquals(parsedAlert, alert);
    }

}
