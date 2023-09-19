/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.List;
import java.util.Set;

public class MLPlatformArchitecturesUtil {

    public static void getNodesOsArchitectures(ThreadPool threadPool, Client client, ActionListener<Set<String>> architecturesListener) {

        ActionListener<NodesInfoResponse> nodesInfoResponseActionListener = ActionListener.wrap(nodesInfoResponse -> {
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
                architecturesListener.onResponse(Set.copyOf(extractMLNodesOsArchitectures(nodesInfoResponse)));
            });
        }, architecturesListener::onFailure);
        getNodesInfoBuilderWithOSAndPlugins(client).execute(nodesInfoResponseActionListener);
    }

    private static List<String> extractMLNodesOsArchitectures(NodesInfoResponse nodesInfoResponse) {
        return nodesInfoResponse.getNodes()
            .stream()
            .filter(node -> node.getNode().hasRole(DiscoveryNodeRole.ML_ROLE.roleName()))
            .map(node -> {
                OsInfo osInfo = node.getInfo(OsInfo.class);
                return Platforms.platformName(osInfo.getName(), osInfo.getArch());
            })
            .toList();
    }

    private static NodesInfoRequestBuilder getNodesInfoBuilderWithOSAndPlugins(Client client) {
        return client.admin().cluster().prepareNodesInfo().clear().setOs(true).setPlugins(true);
    }
}
