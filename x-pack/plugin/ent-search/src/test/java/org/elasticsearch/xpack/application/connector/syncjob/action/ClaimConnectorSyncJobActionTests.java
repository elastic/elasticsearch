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

import java.util.Collections;

import static org.elasticsearch.xpack.application.connector.syncjob.action.ClaimConnectorSyncJobAction.Request;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

public class ClaimConnectorSyncJobActionTests extends ESTestCase {

    public void testValidate_WhenAllFieldsArePresent_ExpectNoValidationError() {
        Request request = new Request(randomAlphaOfLength(10), randomAlphaOfLengthBetween(10, 100), Collections.emptyMap());
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenCursorIsNull_ExpectNoValidationError() {
        Request request = new Request(randomAlphaOfLength(10), randomAlphaOfLengthBetween(10, 100), null);
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenConnectorSyncJobIdIsEmpty_ExpectValidationError() {
        Request request = new Request("", randomAlphaOfLengthBetween(10, 100), null);
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(ConnectorSyncJobConstants.EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE));
    }

    public void testValidate_WhenConnectorSyncJobIdIsNull_ExpectValidationError() {
        Request request = new Request(null, randomAlphaOfLengthBetween(10, 100), null);
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(ConnectorSyncJobConstants.EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE));
    }

    public void testValidate_WhenWorkerHostnameIsNull_ExpectValidationError() {
        Request request = new Request(randomAlphaOfLength(10), null, null);
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(ConnectorSyncJobConstants.EMPTY_WORKER_HOSTNAME_ERROR_MESSAGE));
    }

    public void testValidate_WhenSyncCursorIsEmptyObject_ExpectNoError() {
        Request request = new Request(randomAlphaOfLength(10), randomAlphaOfLength(10), Collections.emptyMap());
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }
}
