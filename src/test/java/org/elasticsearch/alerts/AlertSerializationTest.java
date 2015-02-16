/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import java.io.IOException;

public class AlertSerializationTest extends AbstractAlertingTests {

    @Test
    public void testAlertSerialization() throws Exception {

        Alert alert = createTestAlert("test-serialization");


        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

        final Alert.Parser alertParser =
                internalTestCluster().getInstance(Alert.Parser.class, internalTestCluster().getMasterName());

        Alert parsedAlert = alertParser.parse("test-serialization", true, jsonBuilder.bytes());

        long parsedActionCount = 0;

        long alertActionCount = 0;
        for (Action action : parsedAlert.actions()) {
            boolean found = false;
            ++parsedActionCount;
            alertActionCount = 0;
            for (Action action1 : alert.actions()) {
                ++alertActionCount;
                if (action.type().equals(action1.type())) {
                    assertEqualByGeneratedXContent(action, action1);
                    found = true;
                }
            }
            assertTrue(found);
        }
        assertEquals(parsedActionCount, alertActionCount);

        assertEquals(parsedAlert,alert);

        assertEquals(parsedAlert.status().version(), alert.status().version());
        if (parsedAlert.status().lastExecuted() == null ) {
            assertNull(alert.status().lastExecuted());
        } else {
            assertEquals(parsedAlert.status().lastExecuted().getMillis(), alert.status().lastExecuted().getMillis());
        }
        assertEqualByGeneratedXContent(parsedAlert.schedule(), alert.schedule());
        assertEqualByGeneratedXContent(parsedAlert.trigger(), alert.trigger());
        assertEquals(parsedAlert.throttlePeriod().getMillis(), alert.throttlePeriod().getMillis());
        assertEquals(parsedAlert.status().ackStatus().state(), alert.status().ackStatus().state());
        assertEquals(parsedAlert.metadata().get("foo"), "bar");
    }


    private void assertEqualByGeneratedXContent(ToXContent xCon1, ToXContent xCon2) throws IOException {
        XContentBuilder builder1 = XContentFactory.jsonBuilder();
        XContentBuilder builder2 = XContentFactory.jsonBuilder();
        xCon1.toXContent(builder1, ToXContent.EMPTY_PARAMS);
        xCon2.toXContent(builder2, ToXContent.EMPTY_PARAMS);
        assertEquals(builder1.bytes().toUtf8(), builder2.bytes().toUtf8());

    }

}
