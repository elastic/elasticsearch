/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;

public class XPackUsageAction extends Action<XPackUsageRequest, XPackUsageResponse, XPackUsageRequestBuilder> {

    public static final String NAME = "cluster:monitor/xpack/usage";
    public static final XPackUsageAction INSTANCE = new XPackUsageAction();

    public XPackUsageAction() {
        super(NAME);
    }

    @Override
    public XPackUsageRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new XPackUsageRequestBuilder(client);
    }

    @Override
    public XPackUsageResponse newResponse() {
        return new XPackUsageResponse();
    }
}
