/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.index;

import org.elasticsearch.alerts.client.AlertsClientAction;
import org.elasticsearch.alerts.client.AlertsClientInterface;

/**
 */
public class IndexAlertAction extends AlertsClientAction<IndexAlertRequest, IndexAlertResponse, IndexAlertRequestBuilder> {

    public static final IndexAlertAction INSTANCE = new IndexAlertAction();
    public static final String NAME = "indices:data/write/alert/index";

    private IndexAlertAction() {
        super(NAME);
    }


    @Override
    public IndexAlertRequestBuilder newRequestBuilder(AlertsClientInterface client) {
        return new IndexAlertRequestBuilder(client);
    }

    @Override
    public IndexAlertResponse newResponse() {
        return new IndexAlertResponse();
    }
}
