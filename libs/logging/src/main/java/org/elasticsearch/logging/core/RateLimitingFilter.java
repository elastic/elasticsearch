/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.core;

import org.elasticsearch.logging.DeprecatedMessage;
import org.elasticsearch.logging.ESMapMessage;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class RateLimitingFilter implements Filter {
    private final Set<String> lruKeyCache = Collections.newSetFromMap(Collections.synchronizedMap(new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<String, Boolean> eldest) {
            return size() > 128;
        }
    }));

    private volatile boolean useXOpaqueId = true;

    public RateLimitingFilter() {}

    public void setUseXOpaqueId(boolean useXOpaqueId) {
        this.useXOpaqueId = useXOpaqueId;
    }

    /**
     * Clears the cache of previously-seen keys.
     */
    public void reset() {
        this.lruKeyCache.clear();
    }

    @Override
    public Filter.Result filter(org.elasticsearch.logging.core.LogEvent logEvent) {
        org.elasticsearch.logging.Message message = logEvent.getMessage();
        return filterMessage(message);
    }

    @Override
    public Filter.Result filterMessage(org.elasticsearch.logging.Message message) {
        if (message instanceof final ESMapMessage esLogMessage) { // TODO: just avoid for now
            final String key = getKey(esLogMessage);
            return lruKeyCache.add(key) ? Filter.Result.ACCEPT : Filter.Result.DENY;
        } else {
            return Filter.Result.NEUTRAL;
        }
    }

    private String getKey(ESMapMessage esLogMessage) {
        final String key = esLogMessage.get(DeprecatedMessage.KEY_FIELD_NAME);
        final String productOrigin = esLogMessage.get(DeprecatedMessage.ELASTIC_ORIGIN_FIELD_NAME);
        if (isNullOrEmpty(productOrigin) == false) {
            return productOrigin + key;
        }
        if (useXOpaqueId) {
            String xOpaqueId = esLogMessage.get(DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME);
            return xOpaqueId + key;
        }
        return key;
    }

    // TODO: move to core Strings?
    public static boolean isNullOrEmpty(CharSequence str) {
        return str == null || str.isEmpty();
    }
}
