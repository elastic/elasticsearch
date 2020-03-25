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

package org.elasticsearch.script;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public final class DeprecationMap implements Map<String, Object> {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(DeprecationMap.class));

    private final Map<String, Object> delegate;

    private final Map<String, String> deprecations;

    private final String logKeyPrefix;

    public DeprecationMap(Map<String, Object> delegate, Map<String, String> deprecations, String logKeyPrefix) {
        this.delegate = delegate;
        this.deprecations = deprecations;
        this.logKeyPrefix = logKeyPrefix;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return delegate.containsValue(value);
    }

    @Override
    public Object get(final Object key) {
        String deprecationMessage = deprecations.get(key);
        if (deprecationMessage != null) {
            deprecationLogger.deprecatedAndMaybeLog(logKeyPrefix + "_" + key, deprecationMessage);
        }
        return delegate.get(key);
    }

    @Override
    public Object put(final String key, final Object value) {
        return delegate.put(key, value);
    }

    @Override
    public Object remove(final Object key) {
        return delegate.remove(key);
    }

    @Override
    public void putAll(final Map<? extends String, ?> m) {
        delegate.putAll(m);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Set<String> keySet() {
        return delegate.keySet();
    }

    @Override
    public Collection<Object> values() {
        return delegate.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return delegate.entrySet();
    }
}
