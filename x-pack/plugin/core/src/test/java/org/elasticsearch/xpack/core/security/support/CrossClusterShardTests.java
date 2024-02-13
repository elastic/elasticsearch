/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.indices.mapping.get.TransportGetFieldMappingsIndexAction;
import org.elasticsearch.action.explain.TransportExplainAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportShardMultiGetAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TransportShardMultiTermsVectorAction;
import org.elasticsearch.common.inject.Binding;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
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

    Set<String> MANUALLY_CHECKED_SHARD_ACTIONS = Set.of(
        // The request types for these actions are all subtypes of SingleShardRequest
        TransportShardMultiTermsVectorAction.TYPE.name(),
        TransportExplainAction.TYPE.name(),
        RetentionLeaseActions.ADD.name(),
        RetentionLeaseActions.REMOVE.name(),
        RetentionLeaseActions.RENEW.name(),
        TermVectorsAction.NAME,
        TransportGetAction.TYPE.name(),
        TransportShardMultiGetAction.TYPE.name(),
        TransportGetFieldMappingsIndexAction.TYPE.name(),

        // These actions do not have any references to shard IDs in their requests
        ClusterSearchShardsAction.NAME,
        TransportSearchShardsAction.NAME
    );

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Set.of(LocalStateCompositeXPackPlugin.class);
    }

    @SuppressWarnings("rawtypes")
    public void testCheckForNewShardLevelTransportActions() throws Exception {
        Node node = node();
        List<Binding<TransportAction>> transportActionBindings = node.injector().findBindingsByType(TypeLiteral.get(TransportAction.class));
        Set<String> crossClusterPrivilegeNames = new HashSet<>();
        crossClusterPrivilegeNames.addAll(List.of(CCS_INDICES_PRIVILEGE_NAMES));
        crossClusterPrivilegeNames.addAll(List.of(CCR_INDICES_PRIVILEGE_NAMES));

        List<String> shardActions = transportActionBindings.stream()
            .map(binding -> binding.getProvider().get())
            .filter(action -> IndexPrivilege.get(crossClusterPrivilegeNames).predicate().test(action.actionName))
            .filter(this::actionIsLikelyShardAction)
            .map(action -> action.actionName)
            .toList();

        List<String> actionsNotOnAllowlist = shardActions.stream()
            .filter(Predicate.not(MANUALLY_CHECKED_SHARD_ACTIONS::contains))
            .toList();
        if (actionsNotOnAllowlist.isEmpty() == false) {
            fail("""
                If this test fails, you likely just added a transport action, probably with `shard` in the name. Transport actions which
                operate on shards directly and can be used across clusters must meet some additional requirements in order to be
                handled correctly by all Elasticsearch infrastructure, so please make sure you have read the javadoc on the
                IndicesRequest.RemoteClusterShardRequest interface and implemented it if appropriate and not already appropriately
                implemented by a supertype, then add the name (as in "indices:data/read/get") of your new transport action to
                MANUALLY_CHECKED_SHARD_ACTIONS above. Found actions not in allowlist:
                """ + shardActions);
        }

        // Also make sure the allowlist stays up to date and doesn't have any unnecessary entries.
        List<String> actionsOnAllowlistNotFound = MANUALLY_CHECKED_SHARD_ACTIONS.stream()
            .filter(Predicate.not(shardActions::contains))
            .toList();
        if (actionsOnAllowlistNotFound.isEmpty() == false) {
            fail("Some actions were on the allowlist but not found in the list of cross-cluster capable transport actions, please remove " +
                "these from MANUALLY_CHECKED_SHARD_ACTIONS if they have been removed from Elasticsearch: " + actionsOnAllowlistNotFound);
        }
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
