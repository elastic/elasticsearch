/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.action.admin.cluster.allocation.TransportDeleteDesiredBalanceAction;
import org.elasticsearch.action.admin.cluster.allocation.TransportGetDesiredBalanceAction;
import org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.desirednodes.GetDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.TransportDeleteDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.license.PutLicenseAction;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportRequest;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class DefaultOperatorOnlyRegistry implements OperatorOnlyRegistry {

    public static final Set<String> SIMPLE_ACTIONS = Set.of(
        TransportAddVotingConfigExclusionsAction.TYPE.name(),
        TransportClearVotingConfigExclusionsAction.TYPE.name(),
        PutLicenseAction.NAME,
        "cluster:admin/xpack/license/delete",
        // Autoscaling does not publish its actions to core, literal strings are needed.
        "cluster:admin/autoscaling/put_autoscaling_policy",
        "cluster:admin/autoscaling/delete_autoscaling_policy",
        // Node shutdown APIs are operator only
        "cluster:admin/shutdown/create",
        "cluster:admin/shutdown/get",
        "cluster:admin/shutdown/delete",
        // Node removal prevalidation API
        PrevalidateNodeRemovalAction.NAME,
        // Desired Nodes API
        TransportDeleteDesiredNodesAction.TYPE.name(),
        GetDesiredNodesAction.NAME,
        UpdateDesiredNodesAction.NAME,
        TransportGetDesiredBalanceAction.TYPE.name(),
        TransportDeleteDesiredBalanceAction.TYPE.name()
    );

    private final ClusterSettings clusterSettings;

    public DefaultOperatorOnlyRegistry(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    /**
     * Check whether the given action and request qualify as operator-only. The method returns
     * null if the action+request is NOT operator-only. Other it returns a violation object
     * that contains the message for details.
     */
    public OperatorPrivilegesViolation check(String action, TransportRequest request) {
        if (SIMPLE_ACTIONS.contains(action)) {
            return () -> "action [" + action + "]";
        } else if (ClusterUpdateSettingsAction.NAME.equals(action)) {
            assert request instanceof ClusterUpdateSettingsRequest;
            return checkClusterUpdateSettings((ClusterUpdateSettingsRequest) request);
        } else {
            return null;
        }
    }

    @Override
    public void checkRest(RestHandler restHandler, RestRequest restRequest) {
        // no restrictions
    }

    private OperatorPrivilegesViolation checkClusterUpdateSettings(ClusterUpdateSettingsRequest request) {
        List<String> operatorOnlySettingKeys = Stream.concat(
            request.transientSettings().keySet().stream(),
            request.persistentSettings().keySet().stream()
        ).filter(k -> {
            final Setting<?> setting = clusterSettings.get(k);
            return setting != null && setting.isOperatorOnly();
        }).toList();
        if (false == operatorOnlySettingKeys.isEmpty()) {
            return () -> (operatorOnlySettingKeys.size() == 1 ? "setting" : "settings")
                + " ["
                + Strings.collectionToDelimitedString(operatorOnlySettingKeys, ",")
                + "]";
        } else {
            return null;
        }
    }

}
