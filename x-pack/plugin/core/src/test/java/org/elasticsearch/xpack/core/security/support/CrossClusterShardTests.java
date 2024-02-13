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
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCR_INDICES_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCS_INDICES_PRIVILEGE_NAMES;

public class CrossClusterShardTests extends ESSingleNodeTestCase {

    Set<TransportAction<?, ?>> MANUALLY_CHECKED_SHARD_ACTIONS = Set.of();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Set.of(LocalStateCompositeXPackPlugin.class);
    }

    @SuppressWarnings("rawtypes")
    public void testMe() throws Exception {
        Node node = node();
        List<Binding<TransportAction>> transportActionBindings = node.injector().findBindingsByType(TypeLiteral.get(TransportAction.class));
        Set<String> crossClusterPrivilegeNames = new HashSet<>();
        crossClusterPrivilegeNames.addAll(List.of(CCS_INDICES_PRIVILEGE_NAMES));
        crossClusterPrivilegeNames.addAll(List.of(CCR_INDICES_PRIVILEGE_NAMES));

        List<TransportAction> shardActions = transportActionBindings.stream()
            .map(binding -> binding.getProvider().get())
            .filter(action -> IndexPrivilege.get(crossClusterPrivilegeNames).predicate().test(action.actionName))
            .filter(this::actionIsLikelyShardAction)
            .filter(Predicate.not(MANUALLY_CHECKED_SHARD_ACTIONS::contains))
            .toList();
        shardActions.forEach(action -> {
            logger.info("********************");
            logger.info(action.getClass().getName());
            logger.info(action.actionName);
            assertNotNull(action.actionName);
            assertTrue("""
                If this test fails, you likely just added a transport action, probably with `shard` in the name. Transport actions which
                operate on shards directly and can be used across clusters must meet some additional requirements in order to be
                handled correctly by all Elasticsearch infrastructure, so please make sure you have read the javadoc on the
                IndicesRequest.RemoteClusterShardRequest interface and implemented it if appropriate, then add your new transport
                action (not the request, the transport action) to MANUALLY_CHECKED_SHARD_ACTIONS above.
                """, MANUALLY_CHECKED_SHARD_ACTIONS.contains(action));
        });
    }

    /**
     * Getting to the actual request classes themselves is made difficult by the design of Elasticsearch's transport
     * protocol infrastructure combined with JVM type erasure. Therefore, we resort to a crude heuristic here.
     * @param transportAction The transportport action to be checked.
     * @return True if the action is suspected of being an action which may operate on shards directly.
     */
    private boolean actionIsLikelyShardAction(TransportAction<?, ?> transportAction) {
        Class<?> clazz = transportAction.getClass();
        Set<String> classHeirarchy = new HashSet<>();
        while (clazz != TransportAction.class) {
            classHeirarchy.add(clazz.getName());
            clazz = clazz.getSuperclass();
        }
        return classHeirarchy.stream().anyMatch(className -> className.toLowerCase(Locale.ROOT).contains("shard"))
            || transportAction.actionName.toLowerCase(Locale.ROOT).contains("shard")
            || transportAction.actionName.toLowerCase(Locale.ROOT).contains("[s]");
    }

}
