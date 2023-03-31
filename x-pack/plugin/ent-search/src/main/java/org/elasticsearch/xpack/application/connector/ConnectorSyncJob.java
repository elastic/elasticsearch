/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.xcontent.ParseField;

/**
 * The {@link ConnectorSyncJob} model.
 * Currently only holds the fields we use to construct the connectors sync jobs index
 */
public class ConnectorSyncJob {
    public static final ParseField CANCELATION_REQUESTED_AT_FIELD = new ParseField("cancelation_requested_at");
    public static final ParseField CANCELED_AT_FIELD = new ParseField("canceled_at");
    public static final ParseField COMPLETED_AT = new ParseField("completed_at");
    public static final ParseField CONNECTOR_FIELD = new ParseField("connector");
    public static final ParseField CREATED_AT_FIELD = new ParseField("created_at");
    public static final ParseField DELETED_DOCUMENT_COUNT_FIELD = new ParseField("deleted_document_count");
    public static final ParseField ERROR_FIELD = new ParseField("error");
    public static final ParseField INDEXED_DOCUMENT_COUNT_FIELD = new ParseField("indexed_document_count");
    public static final ParseField INDEXED_DOCUMENT_VOLUME_FIELD = new ParseField("indexed_document_volume");
    public static final ParseField LAST_SEEN_FIELD = new ParseField("last_seen");
    public static final ParseField STARTED_AT_FIELD = new ParseField("started_at");
    public static final ParseField STATUS_FIELD = new ParseField("status");
    public static final ParseField TOTAL_DOCUMENT_COUNT_FIELD = new ParseField("total_document_count");
    public static final ParseField TRIGGER_METHOD_FIELD = new ParseField("trigger_method");
    public static final ParseField WORKER_HOSTNAME_FIELD = new ParseField("worker_hostname");
}
