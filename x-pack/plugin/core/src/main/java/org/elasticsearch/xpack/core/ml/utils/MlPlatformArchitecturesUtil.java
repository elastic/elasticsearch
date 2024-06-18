/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class MlPlatformArchitecturesUtil {

    public static void getMlNodesArchitecturesSet(
        ActionListener<Set<String>> architecturesListener,
        Client client,
        ExecutorService executor
    ) {
        ActionListener<NodesInfoResponse> listener = MlPlatformArchitecturesUtil.getArchitecturesSetFromNodesInfoResponseListener(
            executor,
            architecturesListener
        );

        NodesInfoRequest request = MlPlatformArchitecturesUtil.getNodesInfoBuilderWithMlNodeArchitectureInfo(client).request();
        executeAsyncWithOrigin(client, ML_ORIGIN, TransportNodesInfoAction.TYPE, request, listener);
    }

    static ActionListener<NodesInfoResponse> getArchitecturesSetFromNodesInfoResponseListener(
        ExecutorService executor,
        ActionListener<Set<String>> architecturesListener
    ) {
        return architecturesListener.delegateFailureAndWrap(
            (l, nodesInfoResponse) -> executor.execute(() -> l.onResponse(getArchitecturesSetFromNodesInfoResponse(nodesInfoResponse)))
        );
    }

    static NodesInfoRequestBuilder getNodesInfoBuilderWithMlNodeArchitectureInfo(Client client) {
        return client.admin().cluster().prepareNodesInfo("ml:true").clear().setOs(true).setPlugins(true);
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
        ExecutorService executor,
        TrainedModelConfig configToReturn
    ) {
        String modelID = configToReturn.getModelId();
        String modelPlatformArchitecture = configToReturn.getPlatformArchitecture();

        ActionListener<Set<String>> architecturesListener = successOrFailureListener.delegateFailureAndWrap((l, architectures) -> {
            verifyMlNodesAndModelArchitectures(architectures, modelPlatformArchitecture, modelID);
            l.onResponse(configToReturn);
        });

        getMlNodesArchitecturesSet(architecturesListener, client, executor);
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
