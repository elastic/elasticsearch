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

import com.google.common.base.Objects;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

public class Deallocators implements Deallocator {

    public static final String GRACEFUL_STOP_MIN_AVAILABILITY = "cluster.graceful_stop.min_availability";

    public static class MinAvailability {
        public static final String FULL = "full";
        public static final String PRIMARIES = "primaries";
        public static final String NONE = "none";
    }

    private final AllShardsDeallocator allShardsDeallocator;
    private final PrimariesDeallocator primariesDeallocator;
    private final ClusterService clusterService;

    private AtomicReference<Deallocator> pendingDeallocation = new AtomicReference<>();

    private Deallocator noOpDeallocator = new Deallocator() {
        @Override
        public ListenableFuture<DeallocationResult> deallocate() {
            return Futures.immediateFuture(DeallocationResult.SUCCESS_NOTHING_HAPPENED);
        }

        @Override
        public boolean cancel() {
            return false;
        }

        @Override
        public boolean isDeallocating() {
            return false;
        }

        @Override
        public boolean canDeallocate() {
            return true;
        }

        @Override
        public boolean isNoOp() {
            return true;
        }

        @Override
        public void close() throws IOException {
        }
    };

    @Inject
    public Deallocators(ClusterService clusterService, AllShardsDeallocator allShardsDeallocator, PrimariesDeallocator primariesDeallocator) {
        this.clusterService = clusterService;
        this.allShardsDeallocator = allShardsDeallocator;
        this.primariesDeallocator = primariesDeallocator;
    }

    @Override
    public void close() throws IOException {
        allShardsDeallocator.close();
        primariesDeallocator.close();
    }

    @Override
    public ListenableFuture<Deallocator.DeallocationResult> deallocate() {
        final Deallocator deallocator = deallocator();
        if (!pendingDeallocation.compareAndSet(null, deallocator)) {
            throw new IllegalStateException("Node already deallocating");
        }
        ListenableFuture<DeallocationResult> future = deallocator.deallocate();
        Futures.addCallback(future, new FutureCallback<DeallocationResult>() {
            @Override
            public void onSuccess(DeallocationResult result) {
                pendingDeallocation.compareAndSet(deallocator, null);
            }

            @Override
            public void onFailure(Throwable throwable) {
                pendingDeallocation.compareAndSet(deallocator, null);
            }
        });
        return future;
    }

    @Override
    public boolean cancel() {
        Deallocator deallocator = pendingDeallocation.getAndSet(null);
        return deallocator != null && deallocator.cancel();
    }

    @Override
    public boolean isDeallocating() {
        Deallocator deallocator = Objects.firstNonNull(pendingDeallocation.get(), deallocator());
        return deallocator.isDeallocating();
    }

    @Override
    public boolean canDeallocate() {
        Deallocator deallocator = Objects.firstNonNull(pendingDeallocation.get(), deallocator());
        return deallocator.canDeallocate();
    }

    @Override
    public boolean isNoOp() {
        Deallocator deallocator = Objects.firstNonNull(pendingDeallocation.get(), deallocator());
        return deallocator.isNoOp();
    }

    /**
     * get deallocator according to current min_availability setting.
     * this might not be the one this node is currently deallocating with.
     */
    private Deallocator deallocator() {
        Deallocator deallocator;
        String minAvailability = clusterService.state().metaData().settings().get(GRACEFUL_STOP_MIN_AVAILABILITY, MinAvailability.PRIMARIES);
        switch (minAvailability) {
            case MinAvailability.PRIMARIES:
                deallocator = primariesDeallocator;
                break;
            case MinAvailability.FULL:
                deallocator = allShardsDeallocator;
                break;
            case MinAvailability.NONE:
                deallocator = noOpDeallocator;
                break;
            default:
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "invalid setting for '%s'", GRACEFUL_STOP_MIN_AVAILABILITY));
        }
        return deallocator;
    }
}
