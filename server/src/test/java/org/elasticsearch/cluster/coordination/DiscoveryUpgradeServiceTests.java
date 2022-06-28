/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;

public class DiscoveryUpgradeServiceTests extends ESTestCase {
    public void testCreateDiscoveryNodeWithImpossiblyHighId() {
        final DiscoveryNode discoveryNode = new DiscoveryNode(
            UUIDs.randomBase64UUID(random()),
            buildNewFakeTransportAddress(),
            Version.CURRENT
        );
        final DiscoveryNode fakeNode = DiscoveryUpgradeService.createDiscoveryNodeWithImpossiblyHighId(discoveryNode);
        assertThat(discoveryNode.getId(), lessThan(fakeNode.getId()));
        assertThat(UUIDs.randomBase64UUID(random()), lessThan(fakeNode.getId()));
        assertThat(fakeNode.getId(), containsString(discoveryNode.getId()));
    }
}
