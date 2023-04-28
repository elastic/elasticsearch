/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

public class TestDiscoveryNode {

    /**
     * Creates a new {@link DiscoveryNode} with the specified id, using a fake transport address
     *
     * @param id               the nodes unique (persistent) node id. This constructor will auto generate a random ephemeral id.
     */
    public static DiscoveryNode create(String id) {
        return new DiscoveryNode(id, ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(), DiscoveryNodeRole.roles(), null);
    }
}
