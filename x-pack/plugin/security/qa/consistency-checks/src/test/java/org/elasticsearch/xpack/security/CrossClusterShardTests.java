/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.rankeval.RankEvalPlugin;
import org.elasticsearch.ingest.IngestTestPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.injection.guice.Binding;
import org.elasticsearch.injection.guice.TypeLiteral;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.autoscaling.Autoscaling;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilegeTests;
import org.elasticsearch.xpack.downsample.Downsample;
import org.elasticsearch.xpack.downsample.DownsampleShardPersistentTaskExecutor;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.frozen.FrozenIndices;
import org.elasticsearch.xpack.graph.Graph;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.profiling.ProfilingPlugin;
import org.elasticsearch.xpack.rollup.Rollup;
import org.elasticsearch.xpack.search.AsyncSearch;
import org.elasticsearch.xpack.slm.SnapshotLifecycle;
import org.elasticsearch.xpack.sql.plugin.SqlPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;

public class CrossClusterShardTests extends ESSingleNodeTestCase {

    Set<String> MANUALLY_CHECKED_SHARD_ACTIONS = Set.of(
        // The request types for these actions are all subtypes of SingleShardRequest, and have been evaluated to make sure their
        // `shards()` methods return the correct thing.
        TransportSearchShardsAction.NAME,

        // These types have had the interface implemented manually.
        DownsampleShardPersistentTaskExecutor.DelegatingAction.NAME,

        // These actions do not have any references to shard IDs in their requests.
        TransportClusterSearchShardsAction.TYPE.name(),

        // forked search_shards for ES|QL
        "indices:data/read/esql/search_shards"
    );

    Set<Class<?>> CHECKED_ABSTRACT_CLASSES = Set.of(
        // This abstract class implements the interface so we can assume all of its subtypes do so properly as well.
        TransportSingleShardAction.class
    );

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        final ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.addAll(
            List.of(
                LocalStateCompositeXPackPlugin.class,
                AnalyticsPlugin.class,
                AsyncSearch.class,
                Autoscaling.class,
                Ccr.class,
                DataStreamsPlugin.class,
                Downsample.class,
                EqlPlugin.class,
                EsqlPlugin.class,
                FrozenIndices.class,
                Graph.class,
                IndexLifecycle.class,
                IngestCommonPlugin.class,
                IngestTestPlugin.class,
                MustachePlugin.class,
                ProfilingPlugin.class,
                RankEvalPlugin.class,
                ReindexPlugin.class,
                Rollup.class,
                SnapshotLifecycle.class,
                SqlPlugin.class
            )
        );
        return plugins;
    }

    @SuppressWarnings("rawtypes")
    public void testCheckForNewShardLevelTransportActions() throws Exception {
        Node node = node();
        List<Binding<TransportAction>> transportActionBindings = node.injector().findBindingsByType(TypeLiteral.get(TransportAction.class));
        Set<String> crossClusterPrivilegeNames = new HashSet<>();
        crossClusterPrivilegeNames.addAll(List.of(CrossClusterApiKeyRoleDescriptorBuilder.CCS_INDICES_PRIVILEGE_NAMES));
        crossClusterPrivilegeNames.addAll(List.of(CrossClusterApiKeyRoleDescriptorBuilder.CCR_INDICES_PRIVILEGE_NAMES));

        List<String> shardActions = transportActionBindings.stream()
            .map(binding -> binding.getProvider().get())
            .filter(
                action -> IndexPrivilegeTests.resolvePrivilegeAndAssertSingleton(crossClusterPrivilegeNames)
                    .predicate()
                    .test(action.actionName)
            )
            .filter(this::actionIsLikelyShardAction)
            .map(action -> action.actionName)
            .toList();

        List<String> actionsNotOnAllowlist = shardActions.stream().filter(Predicate.not(MANUALLY_CHECKED_SHARD_ACTIONS::contains)).toList();
        if (actionsNotOnAllowlist.isEmpty() == false) {
            fail("""
                If this test fails, you likely just added a transport action, probably with `shard` in the name. Transport actions which
                operate on shards directly and can be used across clusters must meet some additional requirements in order to be
                handled correctly by all Elasticsearch infrastructure, so please make sure you have read the javadoc on the
                IndicesRequest.RemoteClusterShardRequest interface and implemented it if appropriate and not already appropriately
                implemented by a supertype, then add the name (as in "indices:data/read/get") of your new transport action to
                MANUALLY_CHECKED_SHARD_ACTIONS above. Found actions not in allowlist:
                """ + actionsNotOnAllowlist);
        }

        // Also make sure the allowlist stays up to date and doesn't have any unnecessary entries.
        List<String> actionsOnAllowlistNotFound = MANUALLY_CHECKED_SHARD_ACTIONS.stream()
            .filter(Predicate.not(shardActions::contains))
            .toList();
        if (actionsOnAllowlistNotFound.isEmpty() == false) {
            fail(
                "Some actions were on the allowlist but not found in the list of cross-cluster capable transport actions, please remove "
                    + "these from MANUALLY_CHECKED_SHARD_ACTIONS if they have been removed from Elasticsearch: "
                    + actionsOnAllowlistNotFound
            );
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
        Set<Class<?>> classHeirarchy = new HashSet<>();
        while (clazz != TransportAction.class) {
            classHeirarchy.add(clazz);
            clazz = clazz.getSuperclass();
        }
        boolean hasCheckedSuperclass = classHeirarchy.stream().anyMatch(clz -> CHECKED_ABSTRACT_CLASSES.contains(clz));
        boolean shardInClassName = classHeirarchy.stream().anyMatch(clz -> clz.getName().toLowerCase(Locale.ROOT).contains("shard"));
        return hasCheckedSuperclass == false
            && (shardInClassName
                || transportAction.actionName.toLowerCase(Locale.ROOT).contains("shard")
                || transportAction.actionName.toLowerCase(Locale.ROOT).contains("[s]"));
    }

}
