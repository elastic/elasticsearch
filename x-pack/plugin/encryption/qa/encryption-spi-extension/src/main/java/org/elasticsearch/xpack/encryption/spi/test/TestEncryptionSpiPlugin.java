/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.encryption.spi.test;

import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ActionPlugin.RestHandlersServices;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class TestEncryptionSpiPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, TestEncryptedBlob.TYPE, TestEncryptedBlob::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, TestEncryptedBlob.TYPE, TestEncryptedBlob::readDiffFrom)
        );
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new EncryptionSpiInvocationsRestHandler());
    }
}
