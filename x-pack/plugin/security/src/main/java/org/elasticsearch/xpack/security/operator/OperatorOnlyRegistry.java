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
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.security.transport.filter.IPFilter.IP_FILTER_ENABLED_HTTP_SETTING;
import static org.elasticsearch.xpack.security.transport.filter.IPFilter.IP_FILTER_ENABLED_SETTING;

public class OperatorOnlyRegistry {

    public static final Set<String> SIMPLE_ACTIONS = Set.of(AddVotingConfigExclusionsAction.NAME,
        ClearVotingConfigExclusionsAction.NAME,
        PutLicenseAction.NAME,
        DeleteLicenseAction.NAME,
        // Autoscaling does not publish its actions to core, literal strings are needed.
        "cluster:admin/autoscaling/put_autoscaling_policy",
        "cluster:admin/autoscaling/delete_autoscaling_policy",
        "cluster:admin/autoscaling/get_autoscaling_policy",
        "cluster:admin/autoscaling/get_autoscaling_capacity");

    // This class is a prototype to showcase what it would look like for operator only settings
    // It may not be included in phase 1 delivery. Also this may end up using Enum Property to
    // mark operator only settings instead of using the list here.
    public static final Set<String> SIMPLE_SETTINGS = Set.of(IP_FILTER_ENABLED_HTTP_SETTING.getKey(), IP_FILTER_ENABLED_SETTING.getKey(),
        // TODO: Use literal strings due to dependency. Alternatively we can let each plugin publish names of operator settings
        "xpack.ml.max_machine_memory_percent", "xpack.ml.max_model_memory_limit");

    // This map is just to showcase how "partial" operator-only API would work.
    // It will not be included in phase 1 delivery.
    public static final Map<String, Function<TransportRequest, Supplier<String>>> PARAMETER_SENSITIVE_ACTIONS =
        Map.of(DeleteRepositoryAction.NAME, (request) -> {
            assert request instanceof DeleteRepositoryRequest;
            final DeleteRepositoryRequest deleteRepositoryRequest = (DeleteRepositoryRequest) request;
            if ("found-snapshots".equals(deleteRepositoryRequest.name())) {
                return () -> "action [" + DeleteRepositoryAction.NAME + "] with repository [" + deleteRepositoryRequest.name();
            } else {
                return null;
            }
        });

    // The return type is a bit weird, but it is a shortcut to avoid having to use either
    // a Tuple or a new class to hold true/false and a message/null.
    // Since the combination is either true+message or false+null, it is possible to just
    // use the existence of the message to also indicate whether the result is true or false.
    public Supplier<String> check(String action, TransportRequest request) {
        if (SIMPLE_ACTIONS.contains(action)) {
            return () -> "action [" + action + "]";
        } else if (PARAMETER_SENSITIVE_ACTIONS.containsKey(action)) {
            return PARAMETER_SENSITIVE_ACTIONS.get(action).apply(request);
        } else if (ClusterUpdateSettingsAction.NAME.equals(action)) {
            assert request instanceof ClusterUpdateSettingsRequest;
            final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = (ClusterUpdateSettingsRequest) request;
            return checkSettings(clusterUpdateSettingsRequest);
        } else {
            return null;
        }
    }

    private Supplier<String> checkSettings(ClusterUpdateSettingsRequest clusterUpdateSettingsRequest) {
        final boolean hasEmptyIntersection = Sets.haveEmptyIntersection(
            SIMPLE_SETTINGS, clusterUpdateSettingsRequest.persistentSettings().keySet())
            && Sets.haveEmptyIntersection(SIMPLE_SETTINGS, clusterUpdateSettingsRequest.transientSettings().keySet());

        if (hasEmptyIntersection) {
            return null;
        } else {
            final HashSet<String> requestedSettings = new HashSet<>(clusterUpdateSettingsRequest.persistentSettings().keySet());
            requestedSettings.addAll(clusterUpdateSettingsRequest.transientSettings().keySet());
            requestedSettings.retainAll(SIMPLE_SETTINGS);
            return () -> requestedSettings.size() > 1 ?
                "settings" :
                "setting" + " [" + Strings.collectionToCommaDelimitedString(requestedSettings) + "]";
        }
    }
}

