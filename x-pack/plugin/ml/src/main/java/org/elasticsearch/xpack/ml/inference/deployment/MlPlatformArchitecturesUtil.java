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
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class MlPlatformArchitecturesUtil {

    public static void getMlNodesArchitecturesSet(ActionListener<Set<String>> architecturesListener, Client client, ThreadPool threadPool) {
        ActionListener<NodesInfoResponse> listener = MlPlatformArchitecturesUtil.getArchitecturesSetFromNodesInfoResponseListener(
            threadPool,
            architecturesListener
        );

        NodesInfoRequest request = MlPlatformArchitecturesUtil.getNodesInfoBuilderWithMlNodeArchitectureInfo(client).request();
        executeAsyncWithOrigin(client, ML_ORIGIN, NodesInfoAction.INSTANCE, request, listener);
    }

    static ActionListener<NodesInfoResponse> getArchitecturesSetFromNodesInfoResponseListener(
        ThreadPool threadPool,
        ActionListener<Set<String>> architecturesListener
    ) {
        return ActionListener.wrap(nodesInfoResponse -> {
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
                architecturesListener.onResponse(getArchitecturesSetFromNodesInfoResponse(nodesInfoResponse));
            });
        }, architecturesListener::onFailure);
    }

    static NodesInfoRequestBuilder getNodesInfoBuilderWithMlNodeArchitectureInfo(Client client) {
        return client.admin().cluster().prepareNodesInfo().clear().setNodesIds("ml:true").setOs(true).setPlugins(true);
    }

    private static Set<String> getArchitecturesSetFromNodesInfoResponse(NodesInfoResponse nodesInfoResponse) {
        return nodesInfoResponse.getNodes()
            .stream()
            .filter(node -> node.getNode().hasRole(DiscoveryNodeRole.ML_ROLE.roleName()))
            .map(node -> {
                OsInfo osInfo = node.getInfo(OsInfo.class);
                return Platforms.platformName(osInfo.getName(), osInfo.getArch());
            })
            .collect(Collectors.toUnmodifiableSet());
    }

    public static void verifyMlNodesAndModelArchitectures(
        ActionListener<TrainedModelConfig> successOrFailureListener,
        Client client,
        ThreadPool threadPool,
        TrainedModelConfig configToReturn
    ) {
        String modelID = configToReturn.getModelId();
        String modelPlatformArchitecture = configToReturn.getPlatformArchitecture();

        String modifiedPlatformArchitecture = (modelPlatformArchitecture == null && modelID.contains("linux-x86_64"))
            ? "linux-x86_64"
            : null;
        ActionListener<Set<String>> architecturesListener = ActionListener.wrap((architectures) -> {
            verifyMlNodesAndModelArchitectures(architectures, modifiedPlatformArchitecture, modelID);
            successOrFailureListener.onResponse(configToReturn);
        }, successOrFailureListener::onFailure);

        getMlNodesArchitecturesSet(architecturesListener, client, threadPool);
    }

    static void verifyMlNodesAndModelArchitectures(Set<String> architectures, String modelPlatformArchitecture, String modelID)
        throws IllegalArgumentException, IllegalStateException {

        String architecture = null;
        Iterator<String> architecturesIterator = architectures.iterator();
        // If there are no ML nodes at all in the current cluster we assume that any that are added later will work
        if (modelPlatformArchitecture == null || architectures.isEmpty() || architecturesIterator.hasNext() == false) {
            return;
        }

        if (architectures.size() > 1) {
            throw new IllegalStateException(
                format(
                    "ML nodes in this cluster have multiple platform architectures, but can only have one for this model ([%s]); "
                        + "expected [%s]; "
                        + "but was %s",
                    modelID,
                    modelPlatformArchitecture,
                    architectures
                )
            );
        }

        if (Objects.equals(architecturesIterator.next(), modelPlatformArchitecture) == false) {

            throw new IllegalArgumentException(
                format(
                    "The model being deployed ([%s]) is platform specific and incompatible with ML nodes in the cluster; "
                        + "expected [%s]; "
                        + "but was %s",
                    modelID,
                    modelPlatformArchitecture,
                    architectures
                )
            );
        }
    }
}
