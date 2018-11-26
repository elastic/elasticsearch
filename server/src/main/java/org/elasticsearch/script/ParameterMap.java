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

public final class ParameterMap implements Map<String, Object> {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(ParameterMap.class));

    private final Map<String, Object> params;

    private final Map<String, String> deprecations;

    public ParameterMap(Map<String, Object> params, Map<String, String> deprecations) {
        this.params = params;
        this.deprecations = deprecations;
    }

    @Override
    public int size() {
        return params.size();
    }

    @Override
    public boolean isEmpty() {
        return params.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return params.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return params.containsValue(value);
    }

    @Override
    public Object get(final Object key) {
        String deprecationMessage = deprecations.get(key);
        if (deprecationMessage != null) {
            deprecationLogger.deprecated(deprecationMessage);
        }
        return params.get(key);
    }

    @Override
    public Object put(final String key, final Object value) {
        return params.put(key, value);
    }

    @Override
    public Object remove(final Object key) {
        return params.remove(key);
    }

    @Override
    public void putAll(final Map<? extends String, ?> m) {
        params.putAll(m);
    }

    @Override
    public void clear() {
        params.clear();
    }

    @Override
    public Set<String> keySet() {
        return params.keySet();
    }

    @Override
    public Collection<Object> values() {
        return params.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return params.entrySet();
    }
}
