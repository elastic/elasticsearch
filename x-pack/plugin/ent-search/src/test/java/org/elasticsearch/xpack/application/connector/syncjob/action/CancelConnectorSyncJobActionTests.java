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

import static org.elasticsearch.xpack.application.connector.ConnectorTestUtils.NULL_STRING;
import static org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobConstants.EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CancelConnectorSyncJobActionTests extends ESTestCase {

    public void testValidate_WhenConnectorSyncJobIdIsPresent_ExpectNoValidationError() {
        CancelConnectorSyncJobAction.Request request = ConnectorSyncJobTestUtils.getRandomCancelConnectorSyncJobActionRequest();
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenConnectorSyncJobIdIsEmpty_ExpectValidationError() {
        CancelConnectorSyncJobAction.Request requestWithMissingConnectorId = new CancelConnectorSyncJobAction.Request("");
        ActionRequestValidationException exception = requestWithMissingConnectorId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE));
    }

    public void testValidate_WhenConnectorSyncJobIdIsNull_ExpectValidationError() {
        CancelConnectorSyncJobAction.Request requestWithMissingConnectorId = new CancelConnectorSyncJobAction.Request(NULL_STRING);
        ActionRequestValidationException exception = requestWithMissingConnectorId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE));
    }
}
