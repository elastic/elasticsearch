/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.license.DeleteLicenseRequest;

public class DeleteLicenseRequestBuilder extends AcknowledgedRequestBuilder<
    DeleteLicenseRequest,
    AcknowledgedResponse,
    DeleteLicenseRequestBuilder> {

    public DeleteLicenseRequestBuilder(ElasticsearchClient client) {
        this(client, DeleteLicenseAction.INSTANCE);
    }

    /**
     * Creates new get licenses request builder
     *
     * @param client elasticsearch client
     */
    public DeleteLicenseRequestBuilder(ElasticsearchClient client, DeleteLicenseAction action) {
        super(client, action, new DeleteLicenseRequest());
    }
}
