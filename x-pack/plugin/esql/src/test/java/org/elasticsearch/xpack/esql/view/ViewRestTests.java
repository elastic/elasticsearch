/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.core.TimeValue;

import static org.elasticsearch.xpack.core.esql.EsqlFeatureFlags.ESQL_VIEWS_FEATURE_FLAG;

public class ViewRestTests extends AbstractViewTestCase {

    public void testSnapshot() {
        assumeTrue("Skipping test because we're not in a SNAPSHOT build", ESQL_VIEWS_FEATURE_FLAG.isEnabled());
        GetViewAction.Request request = new GetViewAction.Request(TimeValue.timeValueMinutes(1));
        TestResponseCapture<GetViewAction.Response> responseCapture = new TestResponseCapture<>();
        client().admin().cluster().execute(GetViewAction.INSTANCE, request, responseCapture);
        if (responseCapture.error.get() != null) {
            fail(responseCapture.error.get(), "Failed to get views in SNAPSHOT build");
        }
        if (responseCapture.response == null) {
            fail("Response is null");
        }
    }

    public void testReleased() {
        assumeFalse("Skipping test because we're in a SNAPSHOT build", ESQL_VIEWS_FEATURE_FLAG.isEnabled());
        GetViewAction.Request request = new GetViewAction.Request(TimeValue.timeValueMinutes(1));
        TestResponseCapture<GetViewAction.Response> responseCapture = new TestResponseCapture<>();
        client().admin().cluster().execute(GetViewAction.INSTANCE, request, responseCapture);
        if (responseCapture.error.get() == null) {
            fail("Expected to fail to get views in release build, but no error was returned");
        }
        if (responseCapture.response != null) {
            fail("Received unexpected response in release build: " + responseCapture.response);
        }
    }
}
