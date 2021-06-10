/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

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
     *
     * @return returns {@code true} if the ref count dropped to 0 as a result of calling this method
     */
    boolean decRef();
}
