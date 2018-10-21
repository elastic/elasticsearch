/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;

public class XPackUsageRequestBuilder
        extends MasterNodeOperationRequestBuilder<XPackUsageRequest, XPackUsageResponse, XPackUsageRequestBuilder> {

    public XPackUsageRequestBuilder(ElasticsearchClient client) {
        this(client, XPackUsageAction.INSTANCE);
    }

    public XPackUsageRequestBuilder(ElasticsearchClient client, XPackUsageAction action) {
        super(client, action, new XPackUsageRequest());
    }
}
