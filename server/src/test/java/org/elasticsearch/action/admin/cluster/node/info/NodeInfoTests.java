/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.Map;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link NodeInfo}. Serialization and deserialization tested in
 * {@link org.elasticsearch.nodesinfo.NodeInfoStreamingTests}.
 */
public class NodeInfoTests extends ESTestCase {

    /**
     * Check that the {@link NodeInfo#getInfo(Class)} method returns null
     * for absent info objects, and returns the right thing for present info
     * objects.
     */
    public void testGetInfo() {
        NodeInfo nodeInfo = new NodeInfo(
            Build.current().version(),
            new CompatibilityVersions(TransportVersion.current(), Map.of()),
            IndexVersion.current(),
            Map.of(),
            Build.current(),
            DiscoveryNodeUtils.builder("test_node")
                .roles(emptySet())
                .version(VersionUtils.randomVersion(random()), IndexVersions.ZERO, IndexVersionUtils.randomCompatibleVersion(random()))
                .build(),
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
            null,
            null
        );

        // OsInfo is absent
        assertThat(nodeInfo.getInfo(OsInfo.class), nullValue());

        // JvmInfo is present
        assertThat(nodeInfo.getInfo(JvmInfo.class), notNullValue());
    }
}
