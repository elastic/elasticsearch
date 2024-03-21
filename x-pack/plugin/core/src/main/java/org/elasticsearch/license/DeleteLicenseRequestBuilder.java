/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class DeleteLicenseRequestBuilder extends AcknowledgedRequestBuilder<
    AcknowledgedRequest.Plain,
    AcknowledgedResponse,
    DeleteLicenseRequestBuilder> {

    public DeleteLicenseRequestBuilder(ElasticsearchClient client) {
        super(client, TransportDeleteLicenseAction.TYPE, new AcknowledgedRequest.Plain());
    }
}
