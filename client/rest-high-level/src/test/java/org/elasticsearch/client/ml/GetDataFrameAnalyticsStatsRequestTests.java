/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.containsString;

public class GetDataFrameAnalyticsStatsRequestTests extends ESTestCase {

    public void testValidate_Ok() {
        assertEquals(Optional.empty(), new GetDataFrameAnalyticsStatsRequest("valid-id").validate());
        assertEquals(Optional.empty(), new GetDataFrameAnalyticsStatsRequest("").validate());
    }

    public void testValidate_Failure() {
        assertThat(new GetDataFrameAnalyticsStatsRequest(new String[0]).validate().get().getMessage(),
            containsString("data frame analytics id must not be null"));
    }
}
