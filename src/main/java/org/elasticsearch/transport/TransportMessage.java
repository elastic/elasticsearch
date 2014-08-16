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

package org.elasticsearch.transport;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.transport.TransportAddress;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public abstract class TransportMessage<TM extends TransportMessage<TM>> implements Streamable {

    // a transient (not serialized with the request) key/value registry
    private ObjectObjectOpenHashMap<Object, Object> context;

    private Map<String, Object> headers;

    private TransportAddress remoteAddress;

    protected TransportMessage() {
    }

    protected TransportMessage(TM message) {
        // create a new copy of the headers/context, since we are creating a new request
        // which might have its headers/context changed in the context of that specific request

        if (((TransportMessage<?>) message).headers != null) {
            this.headers = new HashMap<>(((TransportMessage<?>) message).headers);
        }
        if (((TransportMessage<?>) message).context != null) {
            this.context = new ObjectObjectOpenHashMap<>(((TransportMessage<?>) message).context);
        }
    }

    public void remoteAddress(TransportAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public TransportAddress remoteAddress() {
        return remoteAddress;
    }

    @SuppressWarnings("unchecked")
    public final TM putHeader(String key, Object value) {
        if (headers == null) {
            headers = new HashMap<>();
        }
        headers.put(key, value);
        return (TM) this;
    }

    @SuppressWarnings("unchecked")
    public final <V> V getHeader(String key) {
        return headers != null ? (V) headers.get(key) : null;
    }

    public final boolean hasHeader(String key) {
        return headers != null && headers.containsKey(key);
    }

    public Set<String> getHeaders() {
        return headers != null ? headers.keySet() : Collections.<String>emptySet();
    }

    /**
     * Attaches the given transient value to the request - this value will not be serialized
     * along with the request.
     *
     * There are many use cases such data is required, for example, when processing the
     * request headers and building other constructs from them, one could "cache" the
     * already built construct to avoid reprocessing the header over and over again.
     *
     * @return  The previous value that was associated with the given key in the context, or
     *          {@code null} if there was none.
     */
    @SuppressWarnings("unchecked")
    public final synchronized <V> V putInContext(Object key, Object value) {
        if (context == null) {
            context = new ObjectObjectOpenHashMap<>(2);
        }
        return (V) context.put(key, value);
    }

    /**
     * @return  The transient value that is associated with the given key in the request context
     * @see #putInContext(Object, Object)
     */
    @SuppressWarnings("unchecked")
    public final synchronized <V> V getFromContext(Object key) {
        return context != null ? (V) context.get(key) : null;
    }

    /**
     * @param defaultValue  The default value that should be returned for the given key, if no
     *                      value is currently associated with it.
     *
     * @return  The transient value that is associated with the given key in the request context
     *
     * @see #putInContext(Object, Object)
     */
    @SuppressWarnings("unchecked")
    public final synchronized <V> V getFromContext(Object key, V defaultValue) {
        V value = getFromContext(key);
        return value == null ? defaultValue : value;
    }

    /**
     * Checks if the request context contains an entry with the given key
     */
    public final synchronized boolean hasInContext(Object key) {
        return context != null && context.containsKey(key);
    }

    /**
     * @return  The number of transient values attached in the request context.
     */
    public final synchronized int contextSize() {
        return context != null ? context.size() : 0;
    }

    /**
     * Checks if the request context is empty.
     */
    public final synchronized boolean isContextEmpty() {
        return context == null || context.isEmpty();
    }

    /**
     * @return  A safe immutable copy of the current context of this request.
     */
    public synchronized ImmutableOpenMap<Object, Object> getContext() {
        return context != null ? ImmutableOpenMap.of(context) : ImmutableOpenMap.of();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        headers = in.readBoolean() ? in.readMap() : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (headers == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(headers);
        }
    }
}
