/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.action.admin.cluster.allocation.DeleteDesiredBalanceAction;
import org.elasticsearch.action.admin.cluster.allocation.GetDesiredBalanceAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.desirednodes.DeleteDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.GetDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.license.DeleteLicenseAction;
import org.elasticsearch.license.PutLicenseAction;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.transport.TransportRequest;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class DefaultOperatorOnlyRegistry implements OperatorOnlyRegistry {

    public static final Set<String> SIMPLE_ACTIONS = Set.of(
        AddVotingConfigExclusionsAction.NAME,
        ClearVotingConfigExclusionsAction.NAME,
        PutLicenseAction.NAME,
        DeleteLicenseAction.NAME,
        // Autoscaling does not publish its actions to core, literal strings are needed.
        "cluster:admin/autoscaling/put_autoscaling_policy",
        "cluster:admin/autoscaling/delete_autoscaling_policy",
        // Repository analysis actions are not mentioned in core, literal strings are needed.
        "cluster:admin/repository/analyze",
        "cluster:admin/repository/analyze/blob",
        "cluster:admin/repository/analyze/blob/read",
        "cluster:admin/repository/analyze/register",
        // Node shutdown APIs are operator only
        "cluster:admin/shutdown/create",
        "cluster:admin/shutdown/get",
        "cluster:admin/shutdown/delete",
        // Node removal prevalidation API
        PrevalidateNodeRemovalAction.NAME,
        // Desired Nodes API
        DeleteDesiredNodesAction.NAME,
        GetDesiredNodesAction.NAME,
        UpdateDesiredNodesAction.NAME,
        GetDesiredBalanceAction.NAME,
        DeleteDesiredBalanceAction.NAME
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
    public RestResponse checkRestFull(RestHandler restHandler, RestRequest restRequest) {
        return null; // no restrictions
    }

    @Override
    public RestRequest checkRestPartial(RestHandler restHandler, RestRequest restRequest) {
        return restRequest; // no restrictions
    }

    @Override
    public OperatorPrivilegesViolation checkRest(RestHandler restHandler, RestRequest restRequest, RestChannel restChannel) {
        return null; // no restrictions
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
