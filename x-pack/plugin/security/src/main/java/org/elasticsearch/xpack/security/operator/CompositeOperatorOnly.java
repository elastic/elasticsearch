/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.license.DeleteLicenseAction;
import org.elasticsearch.license.PutLicenseAction;
import org.elasticsearch.transport.TransportRequest;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.security.transport.filter.IPFilter.IP_FILTER_ENABLED_HTTP_SETTING;
import static org.elasticsearch.xpack.security.transport.filter.IPFilter.IP_FILTER_ENABLED_SETTING;

public class CompositeOperatorOnly implements OperatorOnly {

    private final List<OperatorOnly> checks;

    public CompositeOperatorOnly() {
        checks = List.of(new ActionOperatorOnly(), new SettingOperatorOnly());
    }

    @Override
    public Result check(String action, TransportRequest request) {
        return checks.stream()
            .map(c -> c.check(action, request))
            .filter(r -> r.getStatus() != Status.CONTINUE)
            .findFirst()
            .orElse(OperatorOnly.RESULT_NO);
    }

    public static final class ActionOperatorOnly implements OperatorOnly {

        public static final Set<String> SIMPLE_ACTIONS = Set.of(
            AddVotingConfigExclusionsAction.NAME,
            ClearVotingConfigExclusionsAction.NAME,
            PutLicenseAction.NAME,
            DeleteLicenseAction.NAME,
            // Autoscaling does not publish its actions to core, literal strings are needed.
            "cluster:admin/autoscaling/put_autoscaling_policy",
            "cluster:admin/autoscaling/delete_autoscaling_policy",
            "cluster:admin/autoscaling/get_autoscaling_policy",
            "cluster:admin/autoscaling/get_autoscaling_capacity"
        );

        // This map is just to showcase how "partial" operator-only API would work.
        // It will not be included in phase 1 delivery.
        public static final Map<String, Function<TransportRequest, Result>> PARAMETER_SENSITIVE_ACTIONS = Map.of(
            DeleteRepositoryAction.NAME, (request) -> {
                assert request instanceof DeleteRepositoryRequest;
                final DeleteRepositoryRequest deleteRepositoryRequest = (DeleteRepositoryRequest) request;
                if ("found-snapshots".equals(deleteRepositoryRequest.name())) {
                    return OperatorOnly.Result.yes(
                        () -> "action [" + DeleteRepositoryAction.NAME + "] with repository [" + deleteRepositoryRequest.name());
                } else {
                    return OperatorOnly.RESULT_NO;
                }
            }
        );

        @Override
        public Result check(String action, TransportRequest request) {
            if (SIMPLE_ACTIONS.contains(action)) {
                return OperatorOnly.Result.yes(() -> "action [" + action + "]");
            } else if (PARAMETER_SENSITIVE_ACTIONS.containsKey(action)) {
                return PARAMETER_SENSITIVE_ACTIONS.get(action).apply(request);
            } else {
                return OperatorOnly.RESULT_CONTINUE;
            }
        }
    }

    // This class is a prototype to showcase what it would look like for operator only settings
    // It may not be included in phase 1 delivery
    public static final class SettingOperatorOnly implements OperatorOnly {

        public static final Set<String> SIMPLE_SETTINGS = Set.of(
            IP_FILTER_ENABLED_HTTP_SETTING.getKey(),
            IP_FILTER_ENABLED_SETTING.getKey(),
            // TODO: Use literal strings due to dependency. Alternatively we can let each plugin publish names of operator settings
            "xpack.ml.max_machine_memory_percent",
            "xpack.ml.max_model_memory_limit"
        );

        @Override
        public Result check(String action, TransportRequest request) {
            if (false == ClusterUpdateSettingsAction.NAME.equals(action)) {
                return OperatorOnly.RESULT_CONTINUE;
            }
            assert request instanceof ClusterUpdateSettingsRequest;
            final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = (ClusterUpdateSettingsRequest) request;

            final boolean hasNoOverlap =
                Sets.haveEmptyIntersection(SIMPLE_SETTINGS, clusterUpdateSettingsRequest.persistentSettings().keySet())
                    && Sets.haveEmptyIntersection(SIMPLE_SETTINGS, clusterUpdateSettingsRequest.transientSettings().keySet());

            if (hasNoOverlap) {
                return OperatorOnly.RESULT_NO;
            } else {
                final HashSet<String> requestedSettings = new HashSet<>(clusterUpdateSettingsRequest.persistentSettings().keySet());
                requestedSettings.addAll(clusterUpdateSettingsRequest.transientSettings().keySet());
                requestedSettings.retainAll(SIMPLE_SETTINGS);
                return OperatorOnly.Result.yes(
                    () -> requestedSettings.size() > 1 ? "settings" : "setting"
                        +" [" + Strings.collectionToCommaDelimitedString(requestedSettings) + "]");
            }
        }
    }
}

