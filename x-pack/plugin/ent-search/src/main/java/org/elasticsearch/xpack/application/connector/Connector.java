/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.xcontent.ParseField;

/**
 * The {@link Connector} model.
 * Currently only holds the fields we use to construct the connectors index
 */
public class Connector {

    public static final ParseField ERROR_FIELD = new ParseField("error");
    public static final ParseField ID_FIELD = new ParseField("id");
    public static final ParseField INDEX_NAME_FIELD = new ParseField("index_name");
    public static final ParseField IS_NATIVE_FIELD = new ParseField("is_native");
    public static final ParseField LANGUAGE_FIELD = new ParseField("language");
    public static final ParseField LAST_SEEN_FIELD = new ParseField("last_seen");
    public static final ParseField LAST_SYNC_ERROR_FIELD = new ParseField("last_sync_error");
    public static final ParseField LAST_SYNC_STATUS_FIELD = new ParseField("last_sync_status");
    public static final ParseField LAST_SYNCED_FIELD = new ParseField("last_synced");
    public static final ParseField LAST_SYNC_SCHEDULED_AT_FIELD = new ParseField("last_sync_scheduled_at");
    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField SERVICE_TYPE_FIELD = new ParseField("service_type");
    public static final ParseField STATUS_FIELD = new ParseField("status");
    public static final ParseField SYNC_NOW_FIELD = new ParseField("status");

    /**
     * Default public constructor.
     */
    public Connector() {}

}
