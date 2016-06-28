/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.common.inject.Injector;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

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
    default List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        return emptyList();
    }
    /**
     * Action filters added by this plugin.
     */
    default List<Class<? extends ActionFilter>> getActionFilters() {
        return emptyList();
    }
    /**
     * Resolve the dependencies needed to build the actions. Called when and if actions are built (so, not for the transport client).
     */
    default Map<Type, ? extends Object> getActionDependencies() {
        return emptyMap();
    }
    /**
     * Resolve the dependencies needed to build the actions from a guice {@link Injector}. Called when and if actions are built (so, not for
     * the transport client). Prefer {@link #getActionDependencies()} where possible.
     */
    default Map<Type, ? extends Object> getActionDependencies(Injector injector) {
        return getActionDependencies();
    }

    public static final class ActionHandler<Request extends ActionRequest<Request>, Response extends ActionResponse> {
        private final GenericAction<Request, Response> action;
        private final Class<? extends TransportAction<Request, Response>> transportAction;
        private final Class<?>[] supportTransportActions;

        /**
         * Create a record of an action, the {@linkplain TransportAction} that handles it, and any supporting {@linkplain TransportActions}
         * that are needed by that {@linkplain TransportAction}.
         * 
         * @param supportTransportActions
         *            array of supporting actions. These are built from back to front so list actions that rely on other actions in the list
         *            before the actions on which they rely
         */
        public ActionHandler(GenericAction<Request, Response> action, Class<? extends TransportAction<Request, Response>> transportAction,
                Class<?>... supportTransportActions) {
            this.action = action;
            this.transportAction = transportAction;
            this.supportTransportActions = supportTransportActions;
        }

        public GenericAction<Request, Response> getAction() {
            return action;
        }

        public Class<? extends TransportAction<Request, Response>> getTransportAction() {
            return transportAction;
        }

        public Class<?>[] getSupportTransportActions() {
            return supportTransportActions;
        }
    }
}
