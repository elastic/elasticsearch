/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.license.GetLicenseRequest;

public class GetLicenseRequestBuilder extends MasterNodeReadOperationRequestBuilder<
    GetLicenseRequest,
    GetLicenseResponse,
    GetLicenseRequestBuilder> {

    public GetLicenseRequestBuilder(ElasticsearchClient client) {
        this(client, GetLicenseAction.INSTANCE);
    }

    /**
     * Creates new get licenses request builder
     *
     * @param client elasticsearch client
     */
    public GetLicenseRequestBuilder(ElasticsearchClient client, GetLicenseAction action) {
        super(client, action, new GetLicenseRequest());
    }
}
