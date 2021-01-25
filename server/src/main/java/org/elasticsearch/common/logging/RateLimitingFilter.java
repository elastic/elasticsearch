/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.logging.DeprecatedMessage.KEY_FIELD_NAME;
import static org.elasticsearch.common.logging.DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME;

@Plugin(name = "RateLimitingFilter", category = Node.CATEGORY, elementType = Filter.ELEMENT_TYPE)
public class RateLimitingFilter extends AbstractFilter {

    private final Set<String> lruKeyCache = Collections.newSetFromMap(Collections.synchronizedMap(new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<String, Boolean> eldest) {
            return size() > 128;
        }
    }));

    public RateLimitingFilter() {
        this(Result.ACCEPT, Result.DENY);
    }

    public RateLimitingFilter(Result onMatch, Result onMismatch) {
        super(onMatch, onMismatch);
    }

    /**
     * Clears the cache of previously-seen keys.
     */
    public void reset() {
        this.lruKeyCache.clear();
    }

    public Result filter(Message message) {
        if (message instanceof ESLogMessage) {
            final ESLogMessage esLogMessage = (ESLogMessage) message;

            String xOpaqueId = esLogMessage.get(X_OPAQUE_ID_FIELD_NAME);
            final String key = esLogMessage.get(KEY_FIELD_NAME);

            return lruKeyCache.add(xOpaqueId + key) ? Result.ACCEPT : Result.DENY;

        } else {
            return Result.NEUTRAL;
        }
    }

    @Override
    public Result filter(LogEvent event) {
        return filter(event.getMessage());
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
        return filter(msg);
    }

    @PluginFactory
    public static RateLimitingFilter createFilter(
        @PluginAttribute("onMatch") final Result match,
        @PluginAttribute("onMismatch") final Result mismatch
    ) {
        return new RateLimitingFilter(match, mismatch);
    }
}
