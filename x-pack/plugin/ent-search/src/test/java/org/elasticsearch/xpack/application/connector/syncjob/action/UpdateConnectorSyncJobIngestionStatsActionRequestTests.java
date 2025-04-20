/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobTestUtils;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobConstants.EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE;
import static org.elasticsearch.xpack.application.connector.syncjob.action.UpdateConnectorSyncJobIngestionStatsAction.Request.DELETED_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE;
import static org.elasticsearch.xpack.application.connector.syncjob.action.UpdateConnectorSyncJobIngestionStatsAction.Request.INDEXED_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE;
import static org.elasticsearch.xpack.application.connector.syncjob.action.UpdateConnectorSyncJobIngestionStatsAction.Request.INDEXED_DOCUMENT_VOLUME_NEGATIVE_ERROR_MESSAGE;
import static org.elasticsearch.xpack.application.connector.syncjob.action.UpdateConnectorSyncJobIngestionStatsAction.Request.TOTAL_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UpdateConnectorSyncJobIngestionStatsActionRequestTests extends ESTestCase {

    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

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
            Instant.now(),
            null
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
            Instant.now(),
            null
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
            Instant.now(),
            null
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
            Instant.now(),
            null
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
            Instant.now(),
            null
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
            Instant.now(),
            null
        );
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(TOTAL_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE));
    }

    public void testParseRequest_requiredFields_validRequest() throws IOException {
        String requestPayload = XContentHelper.stripWhitespace("""
                {
                    "deleted_document_count": 10,
                    "indexed_document_count": 20,
                    "indexed_document_volume": 1000
                }
            """);

        UpdateConnectorSyncJobIngestionStatsAction.Request request = UpdateConnectorSyncJobIngestionStatsAction.Request.fromXContent(
            XContentHelper.createParser(XContentParserConfiguration.EMPTY, new BytesArray(requestPayload), XContentType.JSON),
            randomUUID()
        );

        assertThat(request.getDeletedDocumentCount(), equalTo(10L));
        assertThat(request.getIndexedDocumentCount(), equalTo(20L));
        assertThat(request.getIndexedDocumentVolume(), equalTo(1000L));
    }

    public void testParseRequest_allFieldsWithoutLastSeen_validRequest() throws IOException {
        String requestPayload = XContentHelper.stripWhitespace("""
                {
                    "deleted_document_count": 10,
                    "indexed_document_count": 20,
                    "indexed_document_volume": 1000,
                    "total_document_count": 55,
                    "metadata": {"key1": 1, "key2": 2}
                }
            """);

        UpdateConnectorSyncJobIngestionStatsAction.Request request = UpdateConnectorSyncJobIngestionStatsAction.Request.fromXContent(
            XContentHelper.createParser(XContentParserConfiguration.EMPTY, new BytesArray(requestPayload), XContentType.JSON),
            randomUUID()
        );

        assertThat(request.getDeletedDocumentCount(), equalTo(10L));
        assertThat(request.getIndexedDocumentCount(), equalTo(20L));
        assertThat(request.getIndexedDocumentVolume(), equalTo(1000L));
        assertThat(request.getTotalDocumentCount(), equalTo(55L));
        assertThat(request.getMetadata(), equalTo(Map.of("key1", 1, "key2", 2)));
    }

    public void testParseRequest_metadataTypeInt_invalidRequest() throws IOException {
        String requestPayload = XContentHelper.stripWhitespace("""
                {
                    "deleted_document_count": 10,
                    "indexed_document_count": 20,
                    "indexed_document_volume": 1000,
                    "metadata": 42
                }
            """);

        expectThrows(
            XContentParseException.class,
            () -> UpdateConnectorSyncJobIngestionStatsAction.Request.fromXContent(
                XContentHelper.createParser(XContentParserConfiguration.EMPTY, new BytesArray(requestPayload), XContentType.JSON),
                randomUUID()
            )
        );
    }

    public void testParseRequest_metadataTypeString_invalidRequest() throws IOException {
        String requestPayload = XContentHelper.stripWhitespace("""
                {
                    "deleted_document_count": 10,
                    "indexed_document_count": 20,
                    "indexed_document_volume": 1000,
                    "metadata": "I'm a wrong metadata type"
                }
            """);

        expectThrows(
            XContentParseException.class,
            () -> UpdateConnectorSyncJobIngestionStatsAction.Request.fromXContent(
                XContentHelper.createParser(XContentParserConfiguration.EMPTY, new BytesArray(requestPayload), XContentType.JSON),
                randomUUID()
            )
        );
    }
}
