/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobConstants;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobTestUtils;

import static org.elasticsearch.xpack.application.connector.ConnectorTestUtils.NULL_STRING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CheckInConnectorSyncJobActionTests extends ESTestCase {

    public void testValidate_WhenConnectorSyncJobIdIsPresent_ExpectNoValidationError() {
        CheckInConnectorSyncJobAction.Request request = ConnectorSyncJobTestUtils.getRandomCheckInConnectorSyncJobActionRequest();
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenConnectorSyncJobIdIsEmpty_ExpectValidationError() {
        CheckInConnectorSyncJobAction.Request requestWithMissingConnectorSyncJobId = new CheckInConnectorSyncJobAction.Request("");
        ActionRequestValidationException exception = requestWithMissingConnectorSyncJobId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(ConnectorSyncJobConstants.EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE));
    }

    public void testValidate_WhenConnectorSyncJobIdIsNull_ExpectValidationError() {
        CheckInConnectorSyncJobAction.Request requestWithMissingConnectorSyncJobId = new CheckInConnectorSyncJobAction.Request(NULL_STRING);
        ActionRequestValidationException exception = requestWithMissingConnectorSyncJobId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(ConnectorSyncJobConstants.EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE));
    }
}
