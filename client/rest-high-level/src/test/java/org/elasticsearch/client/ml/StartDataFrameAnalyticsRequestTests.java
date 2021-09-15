/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.containsString;

public class StartDataFrameAnalyticsRequestTests extends ESTestCase {

    public void testValidate_Ok() {
        assertEquals(Optional.empty(), new StartDataFrameAnalyticsRequest("foo").validate());
        assertEquals(Optional.empty(), new StartDataFrameAnalyticsRequest("foo").setTimeout(null).validate());
        assertEquals(Optional.empty(), new StartDataFrameAnalyticsRequest("foo").setTimeout(TimeValue.ZERO).validate());
    }

    public void testValidate_Failure() {
        assertThat(new StartDataFrameAnalyticsRequest(null).validate().get().getMessage(),
            containsString("data frame analytics id must not be null"));
        assertThat(new StartDataFrameAnalyticsRequest(null).setTimeout(TimeValue.ZERO).validate().get().getMessage(),
            containsString("data frame analytics id must not be null"));
    }
}
