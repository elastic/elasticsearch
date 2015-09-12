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

public interface HasContext {

    /**
     * Attaches the given value to the context.
     *
     * @return  The previous value that was associated with the given key in the context, or
     *          {@code null} if there was none.
     */
    <V> V putInContext(Object key, Object value);

    /**
     * Attaches the given values to the context
     */
    void putAllInContext(ObjectObjectAssociativeContainer<Object, Object> map);

    /**
     * @return  The context value that is associated with the given key
     *
     * @see     #putInContext(Object, Object)
     */
    <V> V getFromContext(Object key);

    /**
     * @param defaultValue  The default value that should be returned for the given key, if no
     *                      value is currently associated with it.
     *
     * @return  The value that is associated with the given key in the context
     *
     * @see     #putInContext(Object, Object)
     */
    <V> V getFromContext(Object key, V defaultValue);

    /**
     * Checks if the context contains an entry with the given key
     */
    boolean hasInContext(Object key);

    /**
     * @return  The number of values attached in the context.
     */
    int contextSize();

    /**
     * Checks if the context is empty.
     */
    boolean isContextEmpty();

    /**
     * @return  A safe immutable copy of the current context.
     */
    ImmutableOpenMap<Object, Object> getContext();

    /**
     * Copies the context from the given context holder to this context holder. Any shared keys between
     * the two context will be overridden by the given context holder.
     */
    void copyContextFrom(HasContext other);
}
