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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * A wrapper around a resource that can be released. Note, release should not be
 * called directly on the resource itself.
 * <p/>
 * <p>Yea, I now, the fact that the resouce itself is releasable basically means that
 * users of this class should take care... .
 *
 * @author kimchy (Shay Banon)
 */
public class NonBlockingAcquirableResource<T extends Releasable> implements AcquirableResource<T> {

    private final T resource;

    private AtomicStampedReference<Boolean> counter = new AtomicStampedReference<Boolean>(false, 0);

    private final AtomicBoolean closed = new AtomicBoolean();

    public NonBlockingAcquirableResource(T resource) {
        this.resource = resource;
    }

    @Override public T resource() {
        return resource;
    }

    @Override public boolean acquire() {
        while (true) {
            int stamp = counter.getStamp();
            boolean result = counter.compareAndSet(false, false, stamp, stamp + 1);
            if (result) {
                return true;
            }
            if (counter.getReference()) {
                return false;
            }
        }
    }

    @Override public void release() {
        while (true) {
            boolean currentReference = counter.getReference();
            int stamp = counter.getStamp();
            boolean result = counter.compareAndSet(currentReference, currentReference, stamp, stamp - 1);
            if (result) {
                if (currentReference && (stamp <= 1)) {
                    close();
                }
                return;
            }
        }
    }

    @Override public void markForClose() {
        while (true) {
            int stamp = counter.getStamp();
            boolean result = counter.compareAndSet(false, true, stamp, stamp);
            if (result) {
                if (stamp <= 0) {
                    close();
                }
                return;
            } else if (counter.getReference()) {
                return;
            }
        }
    }

    @Override public void forceClose() {
        close();
    }

    private void close() {
        if (closed.compareAndSet(false, true)) {
            resource.release();
        }
    }
}