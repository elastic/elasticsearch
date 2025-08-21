/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class LinkedProjectConfigTests extends ESTestCase {

    public void testBuilder() {
        final var config = new LinkedProjectConfig(
            ProjectId.fromId(randomAlphaOfLength(10)),
            ProjectId.fromId(randomAlphaOfLength(10)),
            randomAlphanumericOfLength(20),
            randomPositiveTimeValue(),
            randomFrom(Compression.Enabled.values()),
            randomFrom(Compression.Scheme.values()),
            randomPositiveTimeValue(),
            randomPositiveTimeValue(),
            randomBoolean(),
            randomFrom(RemoteConnectionStrategy.ConnectionStrategy.values()),
            randomIntBetween(1, 10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            node -> true,
            randomList(0, 5, () -> randomAlphaOfLength(10)),
            randomIntBetween(1, 1000)
        );

        final var builtConfig = LinkedProjectConfig.builder()
            .originProjectId(config.originProjectId())
            .linkedProjectId(config.linkedProjectId())
            .linkedProjectAlias(config.linkedProjectAlias())
            .transportConnectTimeout(config.transportConnectTimeout())
            .connectionCompression(config.connectionCompression())
            .connectionCompressionScheme(config.connectionCompressionScheme())
            .clusterPingSchedule(config.clusterPingSchedule())
            .initialConnectionTimeout(config.initialConnectionTimeout())
            .skipUnavailable(config.skipUnavailable())
            .connectionStrategy(config.connectionStrategy())
            .maxNumConnections(config.maxNumConnections())
            .proxyAddress(config.proxyAddress())
            .proxyServerName(config.proxyServerName())
            .sniffNodePredicate(config.sniffNodePredicate())
            .sniffSeedNodes(config.sniffSeedNodes())
            .maxPendingConnectionListeners(config.maxPendingConnectionListeners())
            .build();

        assertThat("expect builder generated config to match the original config", builtConfig, equalTo(config));
    }

    public void testLinkedProjectAliasValidation() {
        assertThrows(IllegalArgumentException.class, () -> LinkedProjectConfig.builder().linkedProjectAlias(null).build());
        assertThrows(IllegalArgumentException.class, () -> LinkedProjectConfig.builder().linkedProjectAlias("").build());
    }
}
