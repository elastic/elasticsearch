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

package org.elasticsearch.common;

import com.carrotsearch.hppc.ObjectObjectAssociativeContainer;

import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.util.Set;

public class DelegatingHasContextAndHeaders implements HasContextAndHeaders {

    private HasContextAndHeaders delegate;

    public DelegatingHasContextAndHeaders(HasContextAndHeaders delegate) {
        this.delegate = delegate;
    }

    @Override
    public <V> void putHeader(String key, V value) {
        delegate.putHeader(key, value);
    }

    @Override
    public void copyContextAndHeadersFrom(HasContextAndHeaders other) {
        delegate.copyContextAndHeadersFrom(other);
    }

    @Override
    public <V> V getHeader(String key) {
        return delegate.getHeader(key);
    }

    @Override
    public boolean hasHeader(String key) {
        return delegate.hasHeader(key);
    }

    @Override
    public <V> V putInContext(Object key, Object value) {
        return delegate.putInContext(key, value);
    }

    @Override
    public Set<String> getHeaders() {
        return delegate.getHeaders();
    }

    @Override
    public void copyHeadersFrom(HasHeaders from) {
        delegate.copyHeadersFrom(from);
    }

    @Override
    public void putAllInContext(ObjectObjectAssociativeContainer<Object, Object> map) {
        delegate.putAllInContext(map);
    }

    @Override
    public <V> V getFromContext(Object key) {
        return delegate.getFromContext(key);
    }

    @Override
    public <V> V getFromContext(Object key, V defaultValue) {
        return delegate.getFromContext(key, defaultValue);
    }

    @Override
    public boolean hasInContext(Object key) {
        return delegate.hasInContext(key);
    }

    @Override
    public int contextSize() {
        return delegate.contextSize();
    }

    @Override
    public boolean isContextEmpty() {
        return delegate.isContextEmpty();
    }

    @Override
    public ImmutableOpenMap<Object, Object> getContext() {
        return delegate.getContext();
    }

    @Override
    public void copyContextFrom(HasContext other) {
        delegate.copyContextFrom(other);
    }


}
