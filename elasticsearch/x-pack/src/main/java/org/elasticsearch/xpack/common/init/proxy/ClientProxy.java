/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.init.proxy;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.shield.InternalClient;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.common.init.LazyInitializable;

/**
 * A lazily initialized proxy to an elasticsearch {@link Client}. Inject this proxy whenever a client
 * needs to injected to be avoid circular dependencies issues.
 */
public class ClientProxy implements LazyInitializable {

    protected InternalClient client;

    @Override
    public void init(Injector injector) {
        this.client = injector.getInstance(InternalClient.class);
    }

    public AdminClient admin() {
        return client.admin();
    }

    public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        client.bulk(preProcess(request), listener);
    }

    public BulkRequestBuilder prepareBulk() {
        return client.prepareBulk();
    }

    protected <M extends TransportMessage> M preProcess(M message) {
        return message;
    }
}
