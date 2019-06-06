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

package org.elasticsearch.common.util.concurrent;

/**
 *  An interface for objects that need to be notified when all reference
 *  to itself are not in user anymore. This implements basic reference counting
 *  for instance if async operations holding on to services that are close concurrently
 *  but should be functional until all async operations have joined
 *  Classes implementing this interface should ref counted at any time ie. if an object is used it's reference count should
 *  be increased before using it by calling #incRef and a corresponding #decRef must be called in a try/finally
 *  block to release the object again ie.:
 * <pre>
 *      inst.incRef();
 *      try {
 *        // use the inst...
 *
 *      } finally {
 *          inst.decRef();
 *      }
 * </pre>
 */
public interface RefCounted {

    /**
     * Increments the refCount of this instance.
     *
     * @see #decRef
     * @see #tryIncRef()
     * @throws IllegalStateException iff the reference counter can not be incremented.
     */
    void incRef();

    /**
     * Tries to increment the refCount of this instance. This method will return {@code true} iff the refCount was
     *
     * @see #decRef()
     * @see #incRef()
     */
    boolean tryIncRef();

    /**
     * Decreases the refCount of this  instance. If the refCount drops to 0, then this
     * instance is considered as closed and should not be used anymore.
     *
     * @see #incRef
     */
    void decRef();

}
