/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.get;

import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class GetLicenseRequestBuilder extends MasterNodeReadOperationRequestBuilder<GetLicenseRequest, GetLicenseResponse,
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