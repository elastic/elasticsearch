/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;

import java.util.EnumSet;

public class XPackInfoRequestBuilder extends ActionRequestBuilder<XPackInfoRequest, XPackInfoResponse> {

    public XPackInfoRequestBuilder(ElasticsearchClient client) {
        super(client, XPackInfoAction.INSTANCE, new XPackInfoRequest());
    }

    public XPackInfoRequestBuilder setVerbose(boolean verbose) {
        request.setVerbose(verbose);
        return this;
    }

    public XPackInfoRequestBuilder setCategories(EnumSet<XPackInfoRequest.Category> categories) {
        request.setCategories(categories);
        return this;
    }
}
