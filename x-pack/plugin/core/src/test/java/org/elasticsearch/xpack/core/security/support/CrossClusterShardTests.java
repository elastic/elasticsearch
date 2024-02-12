/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Binding;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCR_INDICES_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCS_INDICES_PRIVILEGE_NAMES;

public class CrossClusterShardTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Set.of(LocalStateCompositeXPackPlugin.class);
    }

    @SuppressWarnings("rawtypes")
    public void testMe() throws Exception {
        Node node = node();
        List<Binding<TransportAction>> transportActionBindings = node.injector().findBindingsByType(TypeLiteral.get(TransportAction.class));

        transportActionBindings.forEach(b -> {
            TransportAction action = b.getProvider().get();
            String actionName = action.actionName;
            if (actionName.startsWith("indices:")) {
                // The set of all the index actions that permissions allow to cross cluster boundaries
                Set<String> crossClusterPrivilegeNames = new HashSet<>();
                crossClusterPrivilegeNames.addAll(List.of(CCS_INDICES_PRIVILEGE_NAMES));
                crossClusterPrivilegeNames.addAll(List.of(CCR_INDICES_PRIVILEGE_NAMES));
                IndexPrivilege allCrossClusterPrivileges = IndexPrivilege.get(crossClusterPrivilegeNames);
                if (allCrossClusterPrivileges.predicate().test(actionName)) {

                    // Getting to the actual request classes themselves is made difficult by the design of Elasticsearch's transport
                    // protocol infrastructure combined with JVM type erasure. Therefore, we resort to a crude heuristic here.
                    if (action.getClass().getName().toLowerCase(Locale.ROOT).contains("shard")
                        || actionName.toLowerCase(Locale.ROOT).contains("shard")) {
                        logger.info("********************");
                        logger.info(action.getClass().getName());
                        logger.info(actionName);
                        assertNotNull(action.actionName);
                        // TODO: check that these actions implement CrossClusterShardAction
                    } else {
                        logger.info("nope: " + actionName);
                    }
                }
            }

        });
    }

}
