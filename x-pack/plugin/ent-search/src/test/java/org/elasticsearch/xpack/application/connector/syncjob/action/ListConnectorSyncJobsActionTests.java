/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobTestUtils;

import static org.hamcrest.Matchers.nullValue;

public class ListConnectorSyncJobsActionTests extends ESTestCase {

    public void testValidate_WhenPageParamsAreValid_ExpectNoValidationError() {
        ListConnectorSyncJobsAction.Request request = ConnectorSyncJobTestUtils.getRandomListConnectorSyncJobsActionRequest();
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

}
