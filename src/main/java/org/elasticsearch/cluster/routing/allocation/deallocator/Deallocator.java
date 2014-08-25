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


package org.elasticsearch.cluster.routing.allocation.deallocator;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;

/**
 * deallocates shards on one node, moving them to other nodes
 */
public interface Deallocator extends Closeable {

    public static class DeallocationResult {

        public static final DeallocationResult SUCCESS_NOTHING_HAPPENED = new DeallocationResult(true, false);
        public static final DeallocationResult SUCCESS = new DeallocationResult(true, true);
        private final boolean success;
        private final boolean didDeallocate;

        protected DeallocationResult(boolean success, boolean didDeallocate) {
            this.success = success;
            this.didDeallocate = didDeallocate;
        }


        public boolean success() {
            return success;
        }


        public boolean didDeallocate() {
            return didDeallocate;
        }
    }

    /**
     * asynchronously deallocate shard of the local node
     *
     */
    public ListenableFuture<DeallocationResult> deallocate();

    /**
     * cancel a currently running deallocation
     *
     * this is not similar to a rollback, if some shards have already been
     * moved they will not be moved back by this method.
     *
     * @return true if a running deallocation was stopped
     */
    public boolean cancel();

    public boolean isDeallocating();

    /**
     * @return true if this deallocator can finish its job succesfully
     */
    public boolean canDeallocate();

    /**
     * @return true if this deallocator would have nothing to do on deallocate()
     */
    public boolean isNoOp();
}
