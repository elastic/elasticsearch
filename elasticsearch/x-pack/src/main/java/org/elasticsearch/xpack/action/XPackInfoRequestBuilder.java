/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 */
public class XPackInfoRequestBuilder extends ActionRequestBuilder<XPackInfoRequest, XPackInfoResponse, XPackInfoRequestBuilder> {

    public XPackInfoRequestBuilder(ElasticsearchClient client) {
        this(client, XPackInfoAction.INSTANCE);
    }

    public XPackInfoRequestBuilder(ElasticsearchClient client, XPackInfoAction action) {
        super(client, action, new XPackInfoRequest());
    }

}
