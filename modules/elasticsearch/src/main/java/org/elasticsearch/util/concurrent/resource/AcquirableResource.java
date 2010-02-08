/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.concurrent.resource;

import org.elasticsearch.util.lease.Releasable;

/**
 * A wrapper around a resource that can be released. Note, release should not be
 * called directly on the resource itself.
 * <p/>
 * <p>Yea, I now, the fact that the resouce itself is releasable basically means that
 * users of this class should take care... .
 *
 * @author kimchy (Shay Banon)
 */
public interface AcquirableResource<T extends Releasable> {

    T resource();

    /**
     * Acquires the resource, returning <tt>true</tt> if it was acquired.
     */
    boolean acquire();

    /**
     * Releases the resource, will close it if there are no more acquirers and it is marked for close.
     */
    void release();

    /**
     * Marks the resource to be closed. Will close it if there are no current
     * acquires.
     */
    void markForClose();

    /**
     * Forces the resource to be closed, regardless of the number of acquirers.
     */
    void forceClose();
}