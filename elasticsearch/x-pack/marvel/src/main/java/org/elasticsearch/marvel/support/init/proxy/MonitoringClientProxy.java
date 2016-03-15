/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.support.init.proxy;

import org.elasticsearch.client.Client;
import org.elasticsearch.shield.InternalClient;
import org.elasticsearch.xpack.common.init.proxy.ClientProxy;

public class MonitoringClientProxy extends ClientProxy {

    /**
     * Creates a proxy to the given internal client (can be used for testing)
     */
    public static MonitoringClientProxy of(Client client) {
        MonitoringClientProxy proxy = new MonitoringClientProxy();
        proxy.client = client instanceof InternalClient ? (InternalClient) client : new InternalClient.Insecure(client);
        return proxy;
    }
}
