/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.elasticsearch.license.DeleteLicenseAction;
import org.elasticsearch.license.PutLicenseAction;
import org.elasticsearch.transport.TransportRequest;

import java.util.Set;

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

    /**
     * Check whether the given action and request qualify as operator-only. The method returns
     * null if the action+request is NOT operator-only. Other it returns a violation object
     * that contains the message for details.
     */
    public OperatorPrivilegesViolation check(String action, TransportRequest request) {
        if (SIMPLE_ACTIONS.contains(action)) {
            return () -> "action [" + action + "]";
        } else {
            return null;
        }
    }

    @FunctionalInterface
    public interface OperatorPrivilegesViolation {
        String message();
    }
}
