/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHeaderDefinition;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * An additional extension point for {@link Plugin}s that extends Elasticsearch's scripting functionality. Implement it like this:
 * <pre>{@code
 *   {@literal @}Override
 *   public List<ActionHandler<?, ?>> getActions() {
 *       return Arrays.asList(new ActionHandler<>(ReindexAction.INSTANCE, TransportReindexAction.class),
 *               new ActionHandler<>(UpdateByQueryAction.INSTANCE, TransportUpdateByQueryAction.class),
 *               new ActionHandler<>(DeleteByQueryAction.INSTANCE, TransportDeleteByQueryAction.class),
 *               new ActionHandler<>(RethrottleAction.INSTANCE, TransportRethrottleAction.class));
 *   }
 * }</pre>
 */
public interface ActionPlugin {
    /**
     * Actions added by this plugin.
     */
    default List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.emptyList();
    }

    /**
     * Client actions added by this plugin. This defaults to all of the {@linkplain ActionType} in
     * {@linkplain ActionPlugin#getActions()}.
     */
    default List<ActionType<? extends ActionResponse>> getClientActions() {
        return getActions().stream().<ActionType<? extends ActionResponse>>map(a -> a.action).toList();
    }

    /**
     * ActionType filters added by this plugin.
     */
    default List<ActionFilter> getActionFilters() {
        return Collections.emptyList();
    }

    /**
     * Rest handlers added by this plugin.
     */
    default List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return Collections.emptyList();
    }

    /**
     * Returns headers which should be copied through rest requests on to internal requests.
     */
    default Collection<RestHeaderDefinition> getRestHeaders() {
        return Collections.emptyList();
    }

    /**
     * Returns headers which should be copied from internal requests into tasks.
     */
    default Collection<String> getTaskHeaders() {
        return Collections.emptyList();
    }

    /**
     * Returns a function used to wrap each rest request before handling the request.
     * The returned {@link UnaryOperator} is called for every incoming rest request and receives
     * the original rest handler as it's input. This allows adding arbitrary functionality around
     * rest request handlers to do for instance logging or authentication.
     * A simple example of how to only allow GET request is here:
     * <pre>
     * {@code
     *    UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
     *      return originalHandler -> (RestHandler) (request, channel, client) -> {
     *        if (request.method() != Method.GET) {
     *          throw new IllegalStateException("only GET requests are allowed");
     *        }
     *        originalHandler.handleRequest(request, channel, client);
     *      };
     *    }
     * }
     * </pre>
     *
     * Note: Only one installed plugin may implement a rest wrapper.
     */
    default UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
        return null;
    }

    final class ActionHandler<Request extends ActionRequest, Response extends ActionResponse> {
        private final ActionType<Response> action;
        private final Class<? extends TransportAction<Request, Response>> transportAction;

        /**
         * Create a record of an action, the {@linkplain TransportAction} that handles it.
         */
        public ActionHandler(ActionType<Response> action, Class<? extends TransportAction<Request, Response>> transportAction) {
            this.action = action;
            this.transportAction = transportAction;
        }

        public ActionType<Response> getAction() {
            return action;
        }

        public Class<? extends TransportAction<Request, Response>> getTransportAction() {
            return transportAction;
        }

        @Override
        public String toString() {
            return action.name() + " is handled by " + transportAction.getName();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != ActionHandler.class) {
                return false;
            }
            ActionHandler<?, ?> other = (ActionHandler<?, ?>) obj;
            return Objects.equals(action, other.action) && Objects.equals(transportAction, other.transportAction);
        }

        @Override
        public int hashCode() {
            return Objects.hash(action, transportAction);
        }
    }

    /**
     * Returns a collection of validators that are used by {@link RequestValidators} to validate a
     * {@link org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest} before the executing it.
     */
    default Collection<RequestValidators.RequestValidator<PutMappingRequest>> mappingRequestValidators() {
        return Collections.emptyList();
    }

    default Collection<RequestValidators.RequestValidator<IndicesAliasesRequest>> indicesAliasesRequestValidators() {
        return Collections.emptyList();
    }

}
