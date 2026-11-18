/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;

import java.util.Set;

public interface GetBuiltinPrivilegesResponseTranslator {

    /**
     * Boost profile actions only exist in serverless, so these privileges are not listed in stateful, and in serverless
     * only once boost profile is enabled.
     */
    Set<String> BOOST_CLUSTER_PRIVILEGES = Set.of(
        ClusterPrivilegeResolver.READ_BOOST_PROFILES.name(),
        ClusterPrivilegeResolver.READ_BOOST_RULES.name(),
        ClusterPrivilegeResolver.MANAGE_BOOST_PROFILES.name(),
        ClusterPrivilegeResolver.MANAGE_BOOST_RULES.name()
    );

    GetBuiltinPrivilegesResponse translate(GetBuiltinPrivilegesResponse response);

    class Default implements GetBuiltinPrivilegesResponseTranslator {

        public GetBuiltinPrivilegesResponse translate(GetBuiltinPrivilegesResponse response) {
            return response;
        }
    }
}
