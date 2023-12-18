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
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobTriggerMethod;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobType;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PostConnectorSyncJobActionTests extends ESTestCase {

    public void testValidate_WhenConnectorIdIsPresent_ExpectNoValidationError() {
        PostConnectorSyncJobAction.Request request = ConnectorSyncJobTestUtils.getRandomPostConnectorSyncJobActionRequest();
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenConnectorIdIsNull_ExpectValidationError() {
        PostConnectorSyncJobAction.Request requestWithMissingConnectorId = new PostConnectorSyncJobAction.Request(
            null,
            ConnectorSyncJobType.FULL,
            ConnectorSyncJobTriggerMethod.ON_DEMAND
        );
        ActionRequestValidationException exception = requestWithMissingConnectorId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(PostConnectorSyncJobAction.Request.EMPTY_CONNECTOR_ID_ERROR_MESSAGE));
    }

}
