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
package org.elasticsearch.cockroach;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.util.Collections;
import java.util.Map;

/**
 * Components that registers all cockroach actions
 */
public class CockroachActionRegistry extends AbstractComponent {

    private volatile Map<String, CockroachActionHolder> actions = Collections.emptyMap();

    private final Object actionHandlerMutex = new Object();

    public CockroachActionRegistry(Settings settings) {
        super(settings);
    }

    public <Request extends CockroachRequest<Request>, Response extends CockroachResponse>
    void registerCockroachAction(String action,
                                 TransportCockroachAction<Request, Response> cockroachAction,
                                 String executor) {
        registerCockroachAction(new CockroachActionHolder(action, cockroachAction, executor));
    }

    private <Request extends CockroachRequest<Request>, Response extends CockroachResponse> void registerCockroachAction(
        CockroachActionHolder<Request, Response> reg) {

        synchronized (actionHandlerMutex) {
            CockroachActionHolder replaced = actions.get(reg.getAction());
            actions = MapBuilder.newMapBuilder(actions).put(reg.getAction(), reg).immutableMap();
            if (replaced != null) {
                logger.warn("registered two handlers for cockroach action {}, handlers: {}, {}", reg.getAction(), reg, replaced);
            }
        }
    }

    public void removeHandler(String action) {
        synchronized (actionHandlerMutex) {
            actions = MapBuilder.newMapBuilder(actions).remove(action).immutableMap();
        }
    }

    @SuppressWarnings("unchecked")
    public <Request extends CockroachRequest<Request>, Response extends CockroachResponse>
    CockroachActionHolder<Request, Response> getCockroachActionHolderSafe(String action) {
        CockroachActionHolder holder = actions.get(action);
        if (holder == null) {
            throw new IllegalStateException("Unknown cockroach action [" + action + "]");
        }
        return holder;
    }

    public <Request extends CockroachRequest<Request>, Response extends CockroachResponse>
    TransportCockroachAction<Request, Response> getCockroachActionSafe(String action) {
        CockroachActionHolder<Request, Response> holder = getCockroachActionHolderSafe(action);
        return holder.getCockroachAction();
    }

    public static class CockroachActionHolder<Request extends CockroachRequest<Request>, Response extends CockroachResponse> {

        private final String action;
        private final TransportCockroachAction<Request, Response> cockroachAction;
        private final String executor;


        public CockroachActionHolder(String action, TransportCockroachAction<Request, Response> cockroachAction, String executor) {
            this.action = action;
            this.cockroachAction = cockroachAction;
            this.executor = executor;
        }

        public String getAction() {
            return action;
        }

        public TransportCockroachAction<Request, Response> getCockroachAction() {
            return cockroachAction;
        }

        public String getExecutor() {
            return executor;
        }
    }
}
