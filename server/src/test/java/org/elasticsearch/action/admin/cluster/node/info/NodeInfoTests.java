/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link NodeInfo}. Serialization and deserialization tested in
 * {@link org.elasticsearch.nodesinfo.NodeInfoStreamingTests}.
 */
public class NodeInfoTests extends ESTestCase {

    /**
     * Check that the the {@link NodeInfo#getInfo(Class)} method returns null
     * for absent info objects, and returns the right thing for present info
     * objects.
     */
    public void testGetInfo() {
        NodeInfo nodeInfo = new NodeInfo(
            Version.CURRENT,
            Build.CURRENT,
            new DiscoveryNode("test_node", buildNewFakeTransportAddress(), emptyMap(), emptySet(), VersionUtils.randomVersion(random())),
            null,
            null,
            null,
            JvmInfo.jvmInfo(),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        // OsInfo is absent
        assertThat(nodeInfo.getInfo(OsInfo.class), nullValue());

        // JvmInfo is present
        assertThat(nodeInfo.getInfo(JvmInfo.class), notNullValue());
    }
}
