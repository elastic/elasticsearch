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

import java.time.Instant;

import static org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobConstants.EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE;
import static org.elasticsearch.xpack.application.connector.syncjob.action.UpdateConnectorSyncJobIngestionStatsAction.Request.DELETED_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE;
import static org.elasticsearch.xpack.application.connector.syncjob.action.UpdateConnectorSyncJobIngestionStatsAction.Request.INDEXED_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE;
import static org.elasticsearch.xpack.application.connector.syncjob.action.UpdateConnectorSyncJobIngestionStatsAction.Request.INDEXED_DOCUMENT_VOLUME_NEGATIVE_ERROR_MESSAGE;
import static org.elasticsearch.xpack.application.connector.syncjob.action.UpdateConnectorSyncJobIngestionStatsAction.Request.TOTAL_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UpdateConnectorSyncJobIngestionStatsActionRequestTests extends ESTestCase {

    public void testValidate_WhenRequestIsValid_ExpectNoValidationError() {
        UpdateConnectorSyncJobIngestionStatsAction.Request request = ConnectorSyncJobTestUtils
            .getRandomUpdateConnectorSyncJobIngestionStatsActionRequest();
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenConnectorSyncJobIdIsEmpty_ExpectValidationError() {
        UpdateConnectorSyncJobIngestionStatsAction.Request request = new UpdateConnectorSyncJobIngestionStatsAction.Request(
            "",
            0L,
            0L,
            0L,
            0L,
            Instant.now()
        );
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE));
    }

    public void testValidate_WhenConnectorSyncJobIdIsNull_ExpectValidationError() {
        UpdateConnectorSyncJobIngestionStatsAction.Request request = new UpdateConnectorSyncJobIngestionStatsAction.Request(
            null,
            0L,
            0L,
            0L,
            0L,
            Instant.now()
        );
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE));
    }

    public void testValidate_WhenDeletedDocumentCountIsNegative_ExpectValidationError() {
        UpdateConnectorSyncJobIngestionStatsAction.Request request = new UpdateConnectorSyncJobIngestionStatsAction.Request(
            randomAlphaOfLength(10),
            -10L,
            0L,
            0L,
            0L,
            Instant.now()
        );
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(DELETED_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE));
    }

    public void testValidate_WhenIndexedDocumentCountIsNegative_ExpectValidationError() {
        UpdateConnectorSyncJobIngestionStatsAction.Request request = new UpdateConnectorSyncJobIngestionStatsAction.Request(
            randomAlphaOfLength(10),
            0L,
            -10L,
            0L,
            0L,
            Instant.now()
        );
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(INDEXED_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE));
    }

    public void testValidate_WhenIndexedDocumentVolumeIsNegative_ExpectValidationError() {
        UpdateConnectorSyncJobIngestionStatsAction.Request request = new UpdateConnectorSyncJobIngestionStatsAction.Request(
            randomAlphaOfLength(10),
            0L,
            0L,
            -10L,
            0L,
            Instant.now()
        );
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(INDEXED_DOCUMENT_VOLUME_NEGATIVE_ERROR_MESSAGE));
    }

    public void testValidate_WhenTotalDocumentCountIsNegative_ExpectValidationError() {
        UpdateConnectorSyncJobIngestionStatsAction.Request request = new UpdateConnectorSyncJobIngestionStatsAction.Request(
            randomAlphaOfLength(10),
            0L,
            0L,
            0L,
            -10L,
            Instant.now()
        );
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(TOTAL_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE));
    }
}
