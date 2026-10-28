/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.cluster.stateless.local;

import org.elasticsearch.test.cluster.local.DefaultLocalElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.stateless.StatelessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;

import java.util.function.Supplier;

public class DefaultLocalStatelessElasticsearchCluster extends DefaultLocalElasticsearchCluster<
    LocalClusterSpec,
    StatelessLocalClusterHandle> implements StatelessElasticsearchCluster {

    public DefaultLocalStatelessElasticsearchCluster(Supplier<LocalClusterSpec> specProvider, StatelessLocalClusterFactory clusterFactory) {
        super(specProvider, clusterFactory);
    }

    @Override
    public void upgradeNodeToVersion(int index, Version version, boolean forciblyDestroyOldNode) {
        getHandle().upgradeNodeToVersion(index, version, forciblyDestroyOldNode);
    }

    @Override
    public void restartNodeInPlace(int index, boolean forciblyDestroyOldNode) {
        getHandle().restartNodeInPlace(index, forciblyDestroyOldNode);
    }
}
