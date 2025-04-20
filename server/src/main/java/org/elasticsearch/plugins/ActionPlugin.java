/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHeaderDefinition;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * An additional extension point for {@link Plugin}s that extends Elasticsearch's scripting functionality. Implement it like this:
 * <pre>{@code
 *   {@literal @}Override
 *   public List<ActionHandler<?, ?>> getActions() {
 *       return List.of(new ActionHandler(ReindexAction.INSTANCE, TransportReindexAction.class),
 *               new ActionHandler(UpdateByQueryAction.INSTANCE, TransportUpdateByQueryAction.class),
 *               new ActionHandler(DeleteByQueryAction.INSTANCE, TransportDeleteByQueryAction.class),
 *               new ActionHandler(RethrottleAction.INSTANCE, TransportRethrottleAction.class));
 *   }
 * }</pre>
 */
public interface ActionPlugin {
    /**
     * Actions added by this plugin.
     */
    default Collection<ActionHandler> getActions() {
        return Collections.emptyList();
    }

    /**
     * ActionType filters added by this plugin.
     */
    default Collection<ActionFilter> getActionFilters() {
        return Collections.emptyList();
    }

    /**
     * Action filters applying to a single action added by this plugin.
     */
    default Collection<MappedActionFilter> getMappedActionFilters() {
        return Collections.emptyList();
    }

    /**
     * Rest handlers added by this plugin.
     */
    default Collection<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
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

    final class ActionHandler {
        private final ActionType<?> action;
        private final Class<? extends TransportAction<?, ?>> transportAction;

        /**
         * Create a record of an action, the {@linkplain TransportAction} that handles it.
         */
        public ActionHandler(ActionType<?> action, Class<? extends TransportAction<?, ?>> transportAction) {
            this.action = action;
            this.transportAction = transportAction;
        }

        public ActionType<?> getAction() {
            return action;
        }

        public Class<? extends TransportAction<?, ?>> getTransportAction() {
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
            ActionHandler other = (ActionHandler) obj;
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
