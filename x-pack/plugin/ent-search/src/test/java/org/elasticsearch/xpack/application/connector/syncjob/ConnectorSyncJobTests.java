/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.ConnectorSyncStatus;

import java.io.IOException;
import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class ConnectorSyncJobTests extends ESTestCase {

    public void testFromXContent_WithAllFields_AllSet() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
                "cancelation_requested_at": "2023-12-01T14:19:39.394194Z",
                "canceled_at": "2023-12-01T14:19:39.394194Z",
                "completed_at": "2023-12-01T14:19:39.394194Z",
                "connector": {
                    "id": "connector-id",
                    "filtering": {
                        "advanced_snippet": {
                            "created_at": "2023-12-01T14:18:37.397819Z",
                            "updated_at": "2023-12-01T14:18:37.397819Z",
                            "value": {}
                        },
                        "rules": [
                            {
                                "created_at": "2023-12-01T14:18:37.397819Z",
                                "field": "_",
                                "id": "DEFAULT",
                                "order": 0,
                                "policy": "include",
                                "rule": "regex",
                                "updated_at": "2023-12-01T14:18:37.397819Z",
                                "value": ".*"
                            }
                        ],
                        "validation": {
                            "errors": [],
                            "state": "valid"
                        }
                    },
                    "index_name": "search-connector",
                    "language": "english",
                    "pipeline": {
                        "extract_binary_content": true,
                        "name": "search-default-ingestion",
                        "reduce_whitespace": true,
                        "run_ml_inference": false
                    },
                    "service_type": "service type",
                    "configuration": {}
                },
                "created_at": "2023-12-01T14:18:43.07693Z",
                "deleted_document_count": 10,
                "error": "some-error",
                "indexed_document_count": 10,
                "indexed_document_volume": 10,
                "job_type": "full",
                "last_seen": "2023-12-01T14:18:43.07693Z",
                "metadata": {},
                "started_at": "2023-12-01T14:18:43.07693Z",
                "status": "canceling",
                "total_document_count": 0,
                "trigger_method": "scheduled",
                "worker_hostname": "worker-hostname"
            }
            """);

        ConnectorSyncJob syncJob = ConnectorSyncJob.fromXContentBytes(new BytesArray(content), "HIC-JYwB9RqKhB7x_hIE", XContentType.JSON);

        assertThat(syncJob.getCancelationRequestedAt(), equalTo(Instant.parse("2023-12-01T14:19:39.394194Z")));
        assertThat(syncJob.getCanceledAt(), equalTo(Instant.parse("2023-12-01T14:19:39.394194Z")));
        assertThat(syncJob.getCompletedAt(), equalTo(Instant.parse("2023-12-01T14:19:39.394194Z")));

        assertThat(syncJob.getConnector().getConnectorId(), equalTo("connector-id"));
        assertThat(syncJob.getConnector().getSyncJobFiltering().getRules(), hasSize(1));
        assertThat(syncJob.getConnector().getIndexName(), equalTo("search-connector"));
        assertThat(syncJob.getConnector().getLanguage(), equalTo("english"));
        assertThat(syncJob.getConnector().getPipeline(), notNullValue());

        assertThat(syncJob.getCreatedAt(), equalTo(Instant.parse("2023-12-01T14:18:43.07693Z")));
        assertThat(syncJob.getDeletedDocumentCount(), equalTo(10L));
        assertThat(syncJob.getError(), equalTo("some-error"));
        assertThat(syncJob.getId(), equalTo("HIC-JYwB9RqKhB7x_hIE"));
        assertThat(syncJob.getIndexedDocumentCount(), equalTo(10L));
        assertThat(syncJob.getIndexedDocumentVolume(), equalTo(10L));
        assertThat(syncJob.getJobType(), equalTo(ConnectorSyncJobType.FULL));
        assertThat(syncJob.getLastSeen(), equalTo(Instant.parse("2023-12-01T14:18:43.07693Z")));
        assertThat(syncJob.getMetadata(), notNullValue());
        assertThat(syncJob.getStartedAt(), equalTo(Instant.parse("2023-12-01T14:18:43.07693Z")));
        assertThat(syncJob.getStatus(), equalTo(ConnectorSyncStatus.CANCELING));
        assertThat(syncJob.getTotalDocumentCount(), equalTo(0L));
        assertThat(syncJob.getTriggerMethod(), equalTo(ConnectorSyncJobTriggerMethod.SCHEDULED));
        assertThat(syncJob.getWorkerHostname(), equalTo("worker-hostname"));
    }

    public void testFromXContent_WithOnlyNonNullableFieldsSet_DoesNotThrow() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
                "connector": {
                    "id": "connector-id",
                    "filtering": {
                        "advanced_snippet": {
                            "created_at": "2023-12-01T14:18:37.397819Z",
                            "updated_at": "2023-12-01T14:18:37.397819Z",
                            "value": {}
                        },
                        "rules": [
                            {
                                "created_at": "2023-12-01T14:18:37.397819Z",
                                "field": "_",
                                "id": "DEFAULT",
                                "order": 0,
                                "policy": "include",
                                "rule": "regex",
                                "updated_at": "2023-12-01T14:18:37.397819Z",
                                "value": ".*"
                            }
                        ],
                        "validation": {
                            "errors": [],
                            "state": "valid"
                        }
                    },
                    "index_name": "search-connector",
                    "language": "english",
                    "pipeline": {
                        "extract_binary_content": true,
                        "name": "search-default-ingestion",
                        "reduce_whitespace": true,
                        "run_ml_inference": false
                    },
                    "service_type": "service type",
                    "configuration": {}
                },
                "created_at": "2023-12-01T14:18:43.07693Z",
                "deleted_document_count": 10,
                "indexed_document_count": 10,
                "indexed_document_volume": 10,
                "job_type": "full",
                "last_seen": "2023-12-01T14:18:43.07693Z",
                "metadata": {},
                "status": "canceling",
                "total_document_count": 0,
                "trigger_method": "scheduled"
            }
            """);

        ConnectorSyncJob.fromXContentBytes(new BytesArray(content), "HIC-JYwB9RqKhB7x_hIE", XContentType.JSON);
    }

    public void testFromXContent_WithAllNullableFieldsSetToNull_DoesNotThrow() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
                "cancelation_requested_at": null,
                "canceled_at": null,
                "completed_at": null,
                "connector": {
                    "id": "connector-id",
                    "filtering": {
                        "advanced_snippet": {
                            "created_at": "2023-12-01T14:18:37.397819Z",
                            "updated_at": "2023-12-01T14:18:37.397819Z",
                            "value": {}
                        },
                        "rules": [
                            {
                                "created_at": "2023-12-01T14:18:37.397819Z",
                                "field": "_",
                                "id": "DEFAULT",
                                "order": 0,
                                "policy": "include",
                                "rule": "regex",
                                "updated_at": "2023-12-01T14:18:37.397819Z",
                                "value": ".*"
                            }
                        ],
                        "validation": {
                            "errors": [],
                            "state": "valid"
                        }
                    },
                    "index_name": "search-connector",
                    "language": "english",
                    "pipeline": {
                        "extract_binary_content": true,
                        "name": "search-default-ingestion",
                        "reduce_whitespace": true,
                        "run_ml_inference": false
                    },
                    "service_type": "service type",
                    "configuration": {}
                },
                "created_at": "2023-12-01T14:18:43.07693Z",
                "deleted_document_count": 10,
                "error": null,
                "indexed_document_count": 10,
                "indexed_document_volume": 10,
                "job_type": "full",
                "last_seen": null,
                "metadata": {},
                "started_at": null,
                "status": "canceling",
                "total_document_count": null,
                "trigger_method": "scheduled",
                "worker_hostname": null
            }
            """);

        ConnectorSyncJob.fromXContentBytes(new BytesArray(content), "HIC-JYwB9RqKhB7x_hIE", XContentType.JSON);
    }

    public void testSyncJobConnectorFromXContent_WithAllFieldsSet() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
                "id": "connector-id",
                "filtering": {
                    "advanced_snippet": {
                        "created_at": "2023-12-01T14:18:37.397819Z",
                        "updated_at": "2023-12-01T14:18:37.397819Z",
                        "value": {}
                    },
                    "rules": [
                        {
                            "created_at": "2023-12-01T14:18:37.397819Z",
                            "field": "_",
                            "id": "DEFAULT",
                            "order": 0,
                            "policy": "include",
                            "rule": "regex",
                            "updated_at": "2023-12-01T14:18:37.397819Z",
                            "value": ".*"
                        }
                    ],
                    "validation": {
                        "errors": [],
                        "state": "valid"
                    }
                },
                "index_name": "search-connector",
                "language": "english",
                "pipeline": {
                    "extract_binary_content": true,
                    "name": "search-default-ingestion",
                    "reduce_whitespace": true,
                    "run_ml_inference": false
                },
                "service_type": "service type",
                "configuration": {}
            }
            """);

        Connector connector = ConnectorSyncJob.syncJobConnectorFromXContentBytes(new BytesArray(content), null, XContentType.JSON);

        assertThat(connector.getConnectorId(), equalTo("connector-id"));
        assertThat(connector.getSyncJobFiltering().getRules(), hasSize(1));
        assertThat(connector.getIndexName(), equalTo("search-connector"));
        assertThat(connector.getLanguage(), equalTo("english"));
        assertThat(connector.getPipeline(), notNullValue());
        assertThat(connector.getServiceType(), equalTo("service type"));
        assertThat(connector.getConfiguration(), notNullValue());
    }

    public void testSyncJobConnectorFromXContent_WithAllNonOptionalFieldsSet_DoesNotThrow() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
                "id": "connector-id",
                "filtering":  {
                    "advanced_snippet": {
                        "created_at": "2023-12-01T14:18:37.397819Z",
                        "updated_at": "2023-12-01T14:18:37.397819Z",
                        "value": {}
                    },
                    "rules": [
                        {
                            "created_at": "2023-12-01T14:18:37.397819Z",
                            "field": "_",
                            "id": "DEFAULT",
                            "order": 0,
                            "policy": "include",
                            "rule": "regex",
                            "updated_at": "2023-12-01T14:18:37.397819Z",
                            "value": ".*"
                        }
                    ],
                    "validation": {
                        "errors": [],
                        "state": "valid"
                    }
                },
                "index_name": null,
                "language": null,
                "pipeline": null,
                "service_type": null,
                "configuration": {}
            }
            """);

        ConnectorSyncJob.syncJobConnectorFromXContentBytes(new BytesArray(content), null, XContentType.JSON);
    }
}
