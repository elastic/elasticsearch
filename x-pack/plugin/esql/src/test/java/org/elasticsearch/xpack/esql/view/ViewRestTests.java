/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.Build;
import org.elasticsearch.core.TimeValue;

public class ViewRestTests extends AbstractViewTestCase {

    public void testRestWorksInBothSnapshotAndRelease() {
        GetViewAction.Request request = new GetViewAction.Request(TimeValue.timeValueMinutes(1));
        TestResponseCapture<GetViewAction.Response> responseCapture = new TestResponseCapture<>();
        client().admin().cluster().execute(GetViewAction.INSTANCE, request, responseCapture);
        if (responseCapture.error.get() != null) {
            String build = Build.current().isSnapshot() ? "SNAPSHOT" : "RELEASE";
            fail(responseCapture.error.get(), "Failed to get views in " + build + " build");
        }
        if (responseCapture.response == null) {
            fail("Response is null");
        }
    }
}
