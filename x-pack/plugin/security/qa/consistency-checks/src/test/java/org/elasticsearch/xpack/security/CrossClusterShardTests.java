/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Binding;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.rankeval.RankEvalPlugin;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.ingest.IngestTestPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.autoscaling.Autoscaling;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.downsample.Downsample;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.frozen.FrozenIndices;
import org.elasticsearch.xpack.graph.Graph;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.profiling.ProfilingPlugin;
import org.elasticsearch.xpack.rollup.Rollup;
import org.elasticsearch.xpack.search.AsyncSearch;
import org.elasticsearch.xpack.slm.SnapshotLifecycle;
import org.elasticsearch.xpack.sql.plugin.SqlPlugin;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This test helps to ensure actions available via RCS 2.0 (api key based cross cluster security) are correctly marked with the
 * IndicesRequest.RemoteClusterShardRequest interface.
 * This interface is used to identify transport actions and request handlers that operate on shards directly and can be used across
 * clusters. This test will fail if a new transport action or request handler is added that operates on shards directly and is not
 * marked with the IndicesRequest.RemoteClusterShardRequest interface.
 * This is a best effort and not a guarantee that all transport actions and request handlers are correctly marked.
 */
public class CrossClusterShardTests extends ESSingleNodeTestCase {

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
                InferencePlugin.class,
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

        Reflections reflections = new Reflections(
            new ConfigurationBuilder().forPackages("org.elasticsearch").addScanners(Scanners.SubTypes).setParallel(false)
        );

        // Find all subclasses of IndicesRequest
        Set<Class<? extends IndicesRequest>> indicesRequest = reflections.getSubTypesOf(IndicesRequest.class);

        // Ignore any indices requests that are already marked with the RemoteClusterShardRequest interface
        Set<Class<? extends IndicesRequest.RemoteClusterShardRequest>> remoteClusterShardRequest = reflections.getSubTypesOf(
            IndicesRequest.RemoteClusterShardRequest.class
        );
        indicesRequest.removeAll(remoteClusterShardRequest);

        // Find any IndicesRequest that have methods related to shards, these are the candidate requests for the marker interface
        Set<String> candidateRequests = new HashSet<>();
        for (Class<? extends IndicesRequest> clazz : indicesRequest) {
            for (Method method : clazz.getDeclaredMethods()) {
                // not the most efficient way to check for shard related methods, but it's good enough for this test
                if (method.getName().toLowerCase(Locale.ROOT).contains("shard")) {
                    // only care if the return type is a ShardId or a collection of ShardIds
                    if (ShardId.class.getCanonicalName().equals(getTypeFromMaybeGeneric(method.getGenericReturnType()))) {
                        candidateRequests.add(method.getDeclaringClass().getCanonicalName());
                    }
                }
            }
        }

        // Find all transport actions
        List<Binding<TransportAction>> transportActionBindings = node.injector().findBindingsByType(TypeLiteral.get(TransportAction.class));

        // Find all transport actions that can execute over RCS 2.0
        Set<String> crossClusterPrivilegeNames = new HashSet<>();
        crossClusterPrivilegeNames.addAll(List.of(CrossClusterApiKeyRoleDescriptorBuilder.CCS_INDICES_PRIVILEGE_NAMES));
        crossClusterPrivilegeNames.addAll(List.of(CrossClusterApiKeyRoleDescriptorBuilder.CCR_INDICES_PRIVILEGE_NAMES));
        List<TransportAction> candidateActions = transportActionBindings.stream()
            .map(binding -> binding.getProvider().get())
            .filter(action -> IndexPrivilege.get(crossClusterPrivilegeNames).predicate().test(action.actionName))
            .toList();

        Set<FinalCandidate> actionsWithShardRequests = new HashSet<>();

        // Find any transport actions that have methods related to shards, these are the candidate actions for the marker interface
        for (TransportAction transportAction : candidateActions) {
            String actionRequestType = getTypeFromMaybeGeneric(transportAction.getClass().getGenericSuperclass());
            if (candidateRequests.contains(actionRequestType)) {
                actionsWithShardRequests.add(new FinalCandidate(transportAction.getClass().getCanonicalName(), actionRequestType));
            }
        }

        // Find any TransportRequestHandler by looking at the request type of the messageReceived method
        Set<Class<? extends TransportRequestHandler>> transportRequestHandlers = reflections.getSubTypesOf(TransportRequestHandler.class);
        for (Class<? extends TransportRequestHandler> transportRequestHandler : transportRequestHandlers) {
            for (Method method : transportRequestHandler.getDeclaredMethods()) {
                if (method.getName().equals("messageReceived")) {
                    // first parameter is the resolved generic type of the TransportRequestHandler
                    Parameter firstParameter = method.getParameters()[0];
                    String actionRequestType = firstParameter.getType().getCanonicalName();
                    if (candidateRequests.contains(actionRequestType)) {
                        actionsWithShardRequests.add(new FinalCandidate(transportRequestHandler.getCanonicalName(), actionRequestType));
                    }
                }
            }
        }

        // Fail if we find any requests that should have the interface
        if (actionsWithShardRequests.isEmpty() == false) {
            fail(
                String.format(
                    """
                        This test failed. You likely just added an index level transport action(s) or transport request handler(s)
                        [%s]
                        with an associated TransportRequest with `shard` in a method name. Transport actions or transport request handlers
                        which operate directly on shards and can be used across clusters must meet some additional requirements in order to
                        be handled correctly by the Elasticsearch security infrastructure. Please review the javadoc for
                        IndicesRequest.RemoteClusterShardRequest and implement the interface on the transport request(s)
                        [%s]
                        """,
                    actionsWithShardRequests.stream().map(FinalCandidate::actionClassName).collect(Collectors.joining(", ")),
                    actionsWithShardRequests.stream().map(FinalCandidate::requestClassName).collect(Collectors.joining(", "))
                )
            );
        }

        // Look for any requests that have the interface but should not
        Set<String> existingRequests = remoteClusterShardRequest.stream().map(Class::getCanonicalName).collect(Collectors.toSet());
        for (Class<? extends IndicesRequest.RemoteClusterShardRequest> clazz : remoteClusterShardRequest) {
            removeExistingRequests(clazz, existingRequests, null);
        }

        // Fail if we find any requests that should not have the interface
        if (existingRequests.isEmpty() == false) {
            fail(String.format("""
                This test failed. You likely just implemented IndicesRequest.RemoteClusterShardRequest where it is not needed.
                This interface is only needed for requests associated with actions that are allowed to go across clusters via the
                API key based (RCS 2.0) cross cluster implementation. You should remove the interface from the following requests:
                [%s]
                """, String.join(", ", existingRequests)));
        }
    }

    /**
     * @return the set of classes that have the interface, but should not.
     */
    private static Set<String> removeExistingRequests(Class<?> clazzToCheck, Set<String> existingRequests, Class<?> originalClazz) {
        if (originalClazz == null) {
            originalClazz = clazzToCheck;
        }
        for (Method method : clazzToCheck.getDeclaredMethods()) {
            // not the most efficient way to check for shard related methods, but it's good enough for this test
            if (method.getName().toLowerCase(Locale.ROOT).contains("shard")) {
                // only care if the return type is a ShardId or a collection of ShardIds
                if (ShardId.class.getCanonicalName().equals(getTypeFromMaybeGeneric(method.getGenericReturnType()))) {
                    existingRequests.remove(originalClazz.getCanonicalName());
                }
            }
        }

        if (clazzToCheck.getSuperclass() == null) {
            return existingRequests;
        } else {
            // check parents too
            removeExistingRequests(clazzToCheck.getSuperclass(), existingRequests, originalClazz);
        }
        return existingRequests;
    }

    /**
     * @return The canonical class name of the first parameter type of a generic type,
     * or the canonical class name of the class if it's not a generic type
     */
    private static String getTypeFromMaybeGeneric(Type type) {
        if (type instanceof ParameterizedType parameterizedType) {
            Type[] typeArguments = parameterizedType.getActualTypeArguments();
            return getTypeFromMaybeGeneric(typeArguments[0]);
        } else if (type instanceof TypeVariable<?>) {
            // too complex to handle this case, and is likely a CRTP pattern which we will catch the children of this class
            return "";
        } else if (type instanceof Class) {
            return ((Class<?>) type).getCanonicalName();
        }
        throw new RuntimeException("Unknown type: " + type.getClass());
    }

    private record FinalCandidate(String actionClassName, String requestClassName) {}
}
