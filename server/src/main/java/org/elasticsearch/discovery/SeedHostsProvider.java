/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.common.transport.TransportAddress;

import java.util.List;

/**
 * A pluggable provider of the list of seed hosts to use for discovery.
 */
public interface SeedHostsProvider {

    /**
     * Returns a list of seed hosts to use for discovery. Called repeatedly while discovery is active (i.e. while there is no master)
     * so that this list may be dynamic.
     */
    List<TransportAddress> getSeedAddresses(HostsResolver hostsResolver);

    /**
     * Helper object that allows to resolve a list of hosts to a list of transport addresses.
     * Each host is resolved into a transport address
     */
    interface HostsResolver {
        List<TransportAddress> resolveHosts(List<String> hosts);
    }
}
