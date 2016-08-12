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
import org.elasticsearch.action.admin.indices.migrate.TransportMigrateIndexAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestHandler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
        return Collections.emptyList();
    }
    /**
     * Action filters added by this plugin.
     */
    default List<Class<? extends ActionFilter>> getActionFilters() {
        return Collections.emptyList();
    }
    /**
     * Rest handlers added by this plugin.
     */
    default List<Class<? extends RestHandler>> getRestHandlers() {
        return Collections.emptyList();
    }

    /**
     * Returns headers which should be copied through rest requests on to internal requests.
     */
    default Collection<String> getRestHeaders() {
        return Collections.emptyList();
    }

    /**
     * Return non-null to provide an implementation for migrating documents during the {@link TransportMigrateIndexAction}. Note that only
     * one installed plugin can do this at a time. If two installed plugins attempt to do this then Elasticsearch will refuse to start up.
     * Note that the reindex <strong>module</strong> provides such an implementation. So if you plan to extend this in a plug you should
     * remove the reindex module or Elasticsearch will fail to start.
     */
    default TransportMigrateIndexAction.DocumentMigrater getDocumentMigrater() {
        return null;
    }

    final class ActionHandler<Request extends ActionRequest<Request>, Response extends ActionResponse> {
        private final GenericAction<Request, Response> action;
        private final Class<? extends TransportAction<Request, Response>> transportAction;
        private final Class<?>[] supportTransportActions;

        /**
         * Create a record of an action, the {@linkplain TransportAction} that handles it, and any supporting {@linkplain TransportActions}
         * that are needed by that {@linkplain TransportAction}.
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

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder().append(action.name()).append(" is handled by ").append(transportAction.getName());
            if (supportTransportActions.length > 0) {
                b.append('[').append(Strings.arrayToCommaDelimitedString(supportTransportActions)).append(']');
            }
            return b.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != ActionHandler.class) {
                return false;
            }
            ActionHandler<?, ?> other = (ActionHandler<?, ?>) obj;
            return Objects.equals(action, other.action)
                    && Objects.equals(transportAction, other.transportAction)
                    && Objects.deepEquals(supportTransportActions, other.supportTransportActions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(action, transportAction, supportTransportActions);
        }
    }
}
