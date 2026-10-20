/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.example.implicit;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;
import org.elasticsearch.xpack.core.security.authz.privilege.ResolvedApplicationPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

/**
 * Mirrors {@code TestImplicitPrivilegesProvider} from the security plugin's internal-cluster
 * integration tests. Any role whose authorization on the {@link #SHIELD_APP} application includes
 * {@link #AGENT_ACTION} implicitly gains {@code read} on {@link #HELICARRIER_INDEX_PATTERN} with a
 * DLS query that scopes results to {@code clearance: public} and an FLS grant that exposes only
 * the {@code clearance} field.
 */
public class HelicarrierImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    public static final String SHIELD_APP = "shield";
    public static final String AGENT_PRIV = "agent";
    public static final String AGENT_ACTION = "data:read/some-action";
    public static final String HELICARRIER_INDEX_PATTERN = "helicarrier-*";
    public static final String HELICARRIER_DLS_QUERY = "{\"term\":{\"clearance\":\"public\"}}";

    @Override
    public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        Collection<ResolvedApplicationPrivilege> applicationPrivileges
    ) {
        if (roleGrantsAgentAction(applicationPrivileges) == false) {
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

    /**
     * True if any resolved grant on {@link #SHIELD_APP} authorizes {@link #AGENT_ACTION}. The resolved
     * {@link ApplicationPrivilege}'s predicate already covers both signals that used to be inspected separately: the
     * action patterns of any stored privilege the role referenced by name, and any raw action patterns written directly
     * under {@code privileges[]} (e.g. {@code "*"} or {@code "data:read/*"}).
     */
    private static boolean roleGrantsAgentAction(Collection<ResolvedApplicationPrivilege> applicationPrivileges) {
        for (ResolvedApplicationPrivilege resolved : applicationPrivileges) {
            final ApplicationPrivilege privilege = resolved.privilege();
            final String application = privilege.getApplication();
            final Predicate<String> appMatches = application.contains("*") ? Automatons.predicate(application) : application::equals;
            if (appMatches.test(SHIELD_APP) && privilege.predicate().test(AGENT_ACTION)) {
                return true;
            }
        }
        return false;
    }
}
