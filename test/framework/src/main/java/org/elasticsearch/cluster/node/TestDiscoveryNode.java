/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class TestDiscoveryNode {

    public static DiscoveryNode create(String id) {
        return new DiscoveryNode(
            "",
            id,
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            null
        );
    }

    public static DiscoveryNode create(String id, TransportAddress address) {
        return new DiscoveryNode("", id, address, Collections.emptyMap(), DiscoveryNodeRole.roles(), null);
    }

    public static DiscoveryNode create(String id, TransportAddress address, Version version) {
        return new DiscoveryNode("", id, address, Collections.emptyMap(), DiscoveryNodeRole.roles(), version);
    }

    public static DiscoveryNode create(String id, TransportAddress address, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode("", id, address, attributes, roles, null);
    }

    public static DiscoveryNode create(
        String id,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        Version version
    ) {
        return new DiscoveryNode("", id, address, attributes, roles, version);
    }
}
