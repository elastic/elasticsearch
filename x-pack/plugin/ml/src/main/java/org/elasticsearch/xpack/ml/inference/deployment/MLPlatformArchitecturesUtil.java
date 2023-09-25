/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class MLPlatformArchitecturesUtil {

    public static void getNodesOsArchitectures(ThreadPool threadPool, Client client, ActionListener<Set<String>> architecturesListener) {

        ActionListener<NodesInfoResponse> nodesInfoResponseActionListener = ActionListener.wrap(nodesInfoResponse -> {
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
                architecturesListener.onResponse(extractMLNodesOsArchitectures(nodesInfoResponse));
            });
        }, architecturesListener::onFailure);

        NodesInfoRequest request = getNodesInfoBuilderWithOSAndPlugins(client).request();

        executeAsyncWithOrigin(client, ML_ORIGIN, NodesInfoAction.INSTANCE, request, nodesInfoResponseActionListener);
    }

    private static Set<String> extractMLNodesOsArchitectures(NodesInfoResponse nodesInfoResponse) {
        return nodesInfoResponse.getNodes()
            .stream()
            .filter(node -> node.getNode().hasRole(DiscoveryNodeRole.ML_ROLE.roleName()))
            .map(node -> {
                OsInfo osInfo = node.getInfo(OsInfo.class);
                return Platforms.platformName(osInfo.getName(), osInfo.getArch());
            })
            .collect(Collectors.toUnmodifiableSet());
    }

    private static NodesInfoRequestBuilder getNodesInfoBuilderWithOSAndPlugins(Client client) {
        return client.admin().cluster().prepareNodesInfo().clear().setNodesIds("ml:true").setOs(true).setPlugins(true);
    }

    public static void verifyArchitectureOfMLNodesIsHomogenous(Set<String> architectures, String requiredArch, String modelId)
        throws IllegalStateException {
        if (architectures.size() > 1) {
            String architecturesStr = architectures.toString();
            architecturesStr = architecturesStr.substring(1, architecturesStr.length() - 1); // Remove the brackets
            throw new IllegalStateException(
                format(
                    "ML nodes in this cluster have multiple platform architectures, but can only have one for this model ([%s]); "
                        + "expected [%s]; "
                        + "but was [%s]",
                    modelId,
                    requiredArch,
                    architecturesStr
                )
            );
        }
    }

    public static void verifyArchitectureMatchesModelPlatformArchitecture(
        Set<String> architectures,
        String modelPlatformArchitecture,
        String modelID
    ) throws IllegalArgumentException {

        String architecture = null;
        Iterator<String> architecturesIterator = architectures.iterator();

        if (architecturesIterator.hasNext()) {
            architecture = architectures.iterator().next();
            if (architectures.size() > 1
                || (Objects.isNull(modelPlatformArchitecture) == false
                    && Objects.equals(architecture, modelPlatformArchitecture) == false)) {

                String architecturesStr = architectures.toString();
                architecturesStr = architecturesStr.substring(1, architecturesStr.length() - 1); // Remove the brackets

                throw new IllegalArgumentException(
                    format(
                        "The model being deployed ([%s]) is platform specific and incompatible with ML nodes in the cluster; "
                            + "expected [%s]; "
                            + "but was [%s]",
                        modelID,
                        modelPlatformArchitecture,
                        architecturesStr
                    )
                );
            }
        }
    }
}
