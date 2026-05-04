/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.example.implicit;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;

import java.util.Collection;
import java.util.List;

/**
 * Mirrors {@code TestImplicitPrivilegesProvider} from the security plugin's internal-cluster
 * integration tests. Any role that holds the {@link #SHIELD_APP}/{@link #AGENT_PRIV}
 * application privilege implicitly gains {@code read} on {@link #HELICARRIER_INDEX_PATTERN}
 * with a DLS query that scopes results to {@code clearance: public} and an FLS grant that
 * exposes only the {@code clearance} field.
 */
public class HelicarrierImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    public static final String SHIELD_APP = "shield";
    public static final String AGENT_PRIV = "agent";
    public static final String HELICARRIER_INDEX_PATTERN = "helicarrier-*";
    public static final String HELICARRIER_DLS_QUERY = "{\"term\":{\"clearance\":\"public\"}}";

    @Override
    public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        RoleDescriptor roleDescriptor,
        Collection<ApplicationPrivilegeDescriptor> storedApplicationPrivileges
    ) {
        final boolean hasQualifyingPrivilege = storedApplicationPrivileges.stream()
            .anyMatch(apd -> SHIELD_APP.equals(apd.getApplication()) && AGENT_PRIV.equals(apd.getName()));
        if (hasQualifyingPrivilege == false) {
            return List.of();
        }
        return List.of(
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(HELICARRIER_INDEX_PATTERN)
                .privileges("read")
                .query(new BytesArray(HELICARRIER_DLS_QUERY))
                .grantedFields("clearance")
                .build()
        );
    }
}
