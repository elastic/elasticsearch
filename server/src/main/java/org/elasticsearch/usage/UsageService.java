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

package org.elasticsearch.usage;

import org.elasticsearch.action.admin.cluster.node.usage.NodeUsage;
import org.elasticsearch.rest.BaseRestHandler;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * A service to monitor usage of Elasticsearch features.
 */
public class UsageService {

    private final Map<String, BaseRestHandler> handlers;

    public UsageService() {
        this.handlers = new HashMap<>();
    }

    /**
     * Add a REST handler to this service.
     *
     * @param handler the {@link BaseRestHandler} to add to the usage service.
     */
    public void addRestHandler(BaseRestHandler handler) {
        Objects.requireNonNull(handler);
        if (handler.getName() == null) {
            throw new IllegalArgumentException("handler of type [" + handler.getClass().getName() + "] does not have a name");
        }
        final BaseRestHandler maybeHandler = handlers.put(handler.getName(), handler);
        /*
         * Handlers will be registered multiple times, once for each route that the handler handles. This means that we will see handlers
         * multiple times, so we do not have a conflict if we are seeing the same instance multiple times. So, we only reject if a handler
         * with the same name was registered before, and it is not the same instance as before.
         */
        if (maybeHandler != null && maybeHandler != handler) {
            final String message = String.format(
                Locale.ROOT,
                "handler of type [%s] conflicts with handler of type [%s] as they both have the same name [%s]",
                handler.getClass().getName(),
                maybeHandler.getClass().getName(),
                handler.getName()
            );
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Get the current usage statistics for this node.
     *
     * @return the {@link NodeUsage} representing the usage statistics for this
     * node
     */
    public Map<String, Long> getRestUsageStats() {
        Map<String, Long> restUsageMap;
        restUsageMap = new HashMap<>();
        handlers.values().forEach(handler -> {
            long usageCount = handler.getUsageCount();
            if (usageCount > 0) {
                restUsageMap.put(handler.getName(), usageCount);
            }
        });
        return restUsageMap;
    }

}
