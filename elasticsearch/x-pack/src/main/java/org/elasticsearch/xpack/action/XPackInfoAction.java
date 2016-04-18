/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 *
 */
public class XPackInfoAction extends Action<XPackInfoRequest, XPackInfoResponse, XPackInfoRequestBuilder> {

    public static final String NAME = "cluster:monitor/xpack/info";
    public static final XPackInfoAction INSTANCE = new XPackInfoAction();

    public XPackInfoAction() {
        super(NAME);
    }

    @Override
    public XPackInfoRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new XPackInfoRequestBuilder(client);
    }

    @Override
    public XPackInfoResponse newResponse() {
        return new XPackInfoResponse();
    }
}
