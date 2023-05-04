/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.transport.TransportAddress;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.elasticsearch.test.ESTestCase.buildNewFakeTransportAddress;

/**
 * Various shortcuts to constructors in {@link DiscoveryNode}.
 */
public class TestDiscoveryNode {

    public static DiscoveryNode create(String id) {
        return create(id, buildNewFakeTransportAddress());
    }

    public static DiscoveryNode create(String name, String id) {
        return create(name, id, buildNewFakeTransportAddress());
    }

    public static DiscoveryNode create(String id, TransportAddress address) {
        return create(null, id, address);
    }

    public static DiscoveryNode create(String name, String id, TransportAddress address) {
        return new DiscoveryNode(
            name,
            id,
            UUIDs.randomBase64UUID(random()),
            address.address().getHostString(),
            address.getAddress(),
            address,
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            null
        );
    }

    public static DiscoveryNode create(String id, Version version) {
        return create(id, buildNewFakeTransportAddress(), version);
    }

    public static DiscoveryNode create(String id, TransportAddress address, Version version) {
        return new DiscoveryNode(
            null,
            id,
            UUIDs.randomBase64UUID(random()),
            address.address().getHostString(),
            address.getAddress(),
            address,
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            version
        );
    }

    public static DiscoveryNode create(String id, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        return create(id, buildNewFakeTransportAddress(), attributes, roles, null);
    }

    public static DiscoveryNode create(String id, TransportAddress address, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        return create(id, address, attributes, roles, null);
    }

    public static DiscoveryNode create(String id, Map<String, String> attributes, Set<DiscoveryNodeRole> roles, Version version) {
        return create(id, buildNewFakeTransportAddress(), attributes, roles, version);
    }

    public static DiscoveryNode create(
        String id,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        Version version
    ) {
        return new DiscoveryNode(
            null,
            id,
            UUIDs.randomBase64UUID(random()),
            address.address().getHostString(),
            address.getAddress(),
            address,
            attributes,
            roles,
            version
        );
    }

    public static DiscoveryNode create(String nodeName, String nodeId, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        return create(nodeName, nodeId, buildNewFakeTransportAddress(), attributes, roles);
    }

    public static DiscoveryNode create(
        String nodeName,
        String nodeId,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles
    ) {
        return new DiscoveryNode(
            nodeName,
            nodeId,
            UUIDs.randomBase64UUID(random()),
            address.address().getHostString(),
            address.getAddress(),
            address,
            attributes,
            roles,
            null
        );
    }

    public static DiscoveryNode create(
        String nodeName,
        String nodeId,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        String externalId
    ) {
        return new DiscoveryNode(
            nodeName,
            nodeId,
            UUIDs.randomBase64UUID(random()),
            address.address().getHostString(),
            address.getAddress(),
            address,
            attributes,
            roles,
            null,
            externalId
        );
    }
}
