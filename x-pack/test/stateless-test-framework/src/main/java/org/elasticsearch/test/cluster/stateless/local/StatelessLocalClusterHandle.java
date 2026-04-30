/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.cluster.stateless.local;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.cluster.local.AbstractLocalClusterFactory.Node;
import org.elasticsearch.test.cluster.local.DefaultLocalClusterHandle;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;
import org.elasticsearch.test.cluster.util.Version;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.function.Predicate.not;

public class StatelessLocalClusterHandle extends DefaultLocalClusterHandle {
    private static final Logger LOGGER = LogManager.getLogger(StatelessLocalClusterHandle.class);

    private final String name;
    private final List<Node> nodes;
    private final Path baseWorkingDir;
    private final DistributionResolver distributionResolver;
    private final Lock nodeLock = new ReentrantLock();

    public StatelessLocalClusterHandle(String name, Path baseWorkingDir, DistributionResolver distributionResolver, List<Node> nodes) {
        super(name, nodes);
        this.name = name;
        this.baseWorkingDir = baseWorkingDir;
        this.distributionResolver = distributionResolver;
        this.nodes = nodes;
    }

    @Override
    public void stopNode(int index, boolean forcibly) {
        nodeLock.lock();
        // When stopping a node in stateless mode we replace it on subsequent startup, similar to container churn behavior.
        Node oldNode = nodes.get(index);
        Node newNode = new StatelessNode(baseWorkingDir, distributionResolver, oldNode.getSpec());

        nodes.set(index, newNode);
        nodeLock.unlock();

        LOGGER.info("Stopping node '{}'. It will be replaced by node '{}' on subsequent startup.", oldNode.getName(), newNode.getName());
        oldNode.stop(forcibly);
    }

    @Override
    public void upgradeNodeToVersion(int index, Version version) {
        upgradeNodeToVersion(index, version, false);
    }

    public void upgradeNodeToVersion(int index, Version version, boolean forciblyDestroyOldNode) {
        nodeLock.lock();
        Node oldNode = nodes.get(index);
        LocalClusterSpec.LocalNodeSpec oldNodeSpec = oldNode.getSpec();
        Node newNode = new StatelessNode(baseWorkingDir, distributionResolver, oldNodeSpec);

        LOGGER.info("Replacing node '{}' with node '{}'", oldNode.getName(), newNode.getName());
        nodes.add(index, newNode);
        newNode.start(version);
        waitUntilReady();
        oldNode.stop(forciblyDestroyOldNode);
        nodes.remove(oldNode);
        waitUntilReady();
        nodeLock.unlock();
    }

    @Override
    public void upgradeToVersion(Version version) {
        LOGGER.info("Upgrading stateless Elasticsearch cluster '{}'", name);

        List<Node> searchNodes = nodes.stream().filter(n -> n.getSpec().hasRole("search")).toList();
        List<Node> otherNodes = nodes.stream().filter(not(n -> n.getSpec().hasRole("search"))).toList();
        searchNodes.forEach(n -> upgradeNodeToVersion(n, version));
        otherNodes.forEach(n -> upgradeNodeToVersion(n, version));
    }

    public void restartNodeInPlace(int index, boolean forcibly) {
        Node node = nodes.get(index);
        node.stop(forcibly);
        node.start(null);
        waitUntilReady();
    }

    private void upgradeNodeToVersion(Node node, Version version) {
        upgradeNodeToVersion(nodes.indexOf(node), version);
    }
}
