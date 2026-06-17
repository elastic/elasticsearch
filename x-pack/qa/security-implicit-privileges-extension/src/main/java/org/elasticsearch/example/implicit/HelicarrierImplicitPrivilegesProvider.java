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
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Arrays;
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
        RoleDescriptor roleDescriptor,
        Collection<ApplicationPrivilegeDescriptor> storedApplicationPrivileges
    ) {
        if (roleGrantsAgentAction(roleDescriptor, storedApplicationPrivileges) == false) {
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
     * True if the role's authorization on {@link #SHIELD_APP} includes {@link #AGENT_ACTION}. Pure
     * action-pattern matching from two complementary sources:
     *
     * <ol>
     *   <li><b>Stored-descriptor actions.</b> Each {@link ApplicationPrivilegeDescriptor} on the shield
     *       application contributes its action patterns. This is the authoritative signal when the role
     *       references a stored privilege by name (the descriptor is resolved upstream and its actions
     *       describe what the role can actually invoke at authorization time).</li>
     *   <li><b>Role-descriptor action patterns.</b> Each {@link RoleDescriptor.ApplicationResourcePrivileges}
     *       entry whose application matches shield contributes its raw privilege strings. This covers the
     *       case where every privilege name is itself an action pattern (e.g. {@code "*"} or
     *       {@code "data:read/*"}); {@code NativePrivilegeStore#getPrivileges} short-circuits to an empty
     *       descriptor set in that case, so the role descriptor is the only signal we have.</li>
     * </ol>
     */
    private static boolean roleGrantsAgentAction(
        RoleDescriptor roleDescriptor,
        Collection<ApplicationPrivilegeDescriptor> storedApplicationPrivileges
    ) {
        for (ApplicationPrivilegeDescriptor apd : storedApplicationPrivileges) {
            if (!SHIELD_APP.equals(apd.getApplication())) {
                continue;
            }
            if (Automatons.predicate(apd.getActions()).test(AGENT_ACTION)) {
                return true;
            }
        }
        for (RoleDescriptor.ApplicationResourcePrivileges arp : roleDescriptor.getApplicationPrivileges()) {
            final Predicate<String> appMatches = arp.getApplication().contains("*")
                ? Automatons.predicate(arp.getApplication())
                : arp.getApplication()::equals;
            if (appMatches.test(SHIELD_APP) == false) {
                continue;
            }
            if (Automatons.predicate(Arrays.asList(arp.getPrivileges())).test(AGENT_ACTION)) {
                return true;
            }
        }
        return false;
    }
}
