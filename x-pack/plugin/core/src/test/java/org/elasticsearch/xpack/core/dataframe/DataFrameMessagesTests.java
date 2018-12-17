/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe;

import org.elasticsearch.test.ESTestCase;

public class DataFrameMessagesTests extends ESTestCase {

    public void testGetMessage_WithFormatStrings() {
        String formattedMessage = DataFrameMessages.getMessage(DataFrameMessages.REST_STOP_JOB_WAIT_FOR_COMPLETION_TIMEOUT, "30s",
                "my_job");
        assertEquals("Timed out after [30s] while waiting for data frame job [my_job] to stop", formattedMessage);
    }
}
