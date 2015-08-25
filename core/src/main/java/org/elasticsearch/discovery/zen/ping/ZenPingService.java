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

package org.elasticsearch.discovery.zen.ping;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ZenPingService extends AbstractLifecycleComponent<ZenPing> implements ZenPing {

    private List<ZenPing> zenPings = Collections.emptyList();

    @Inject
    public ZenPingService(Settings settings, Set<ZenPing> zenPings) {
        super(settings);
        this.zenPings = Collections.unmodifiableList(new ArrayList<>(zenPings));
    }

    public List<ZenPing> zenPings() {
        return this.zenPings;
    }

    @Override
    public void setPingContextProvider(PingContextProvider contextProvider) {
        if (lifecycle.started()) {
            throw new IllegalStateException("Can't set nodes provider when started");
        }
        for (ZenPing zenPing : zenPings) {
            zenPing.setPingContextProvider(contextProvider);
        }
    }

    @Override
    protected void doStart() {
        for (ZenPing zenPing : zenPings) {
            zenPing.start();
        }
    }

    @Override
    protected void doStop() {
        for (ZenPing zenPing : zenPings) {
            zenPing.stop();
        }
    }

    @Override
    protected void doClose() {
        for (ZenPing zenPing : zenPings) {
            zenPing.close();
        }
    }

    public PingResponse[] pingAndWait(TimeValue timeout) {
        final AtomicReference<PingResponse[]> response = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ping(new PingListener() {
            @Override
            public void onPing(PingResponse[] pings) {
                response.set(pings);
                latch.countDown();
            }
        }, timeout);
        try {
            latch.await();
            return response.get();
        } catch (InterruptedException e) {
            logger.trace("pingAndWait interrupted");
            return null;
        }
    }

    @Override
    public void ping(PingListener listener, TimeValue timeout) {
        List<? extends ZenPing> zenPings = this.zenPings;
        CompoundPingListener compoundPingListener = new CompoundPingListener(listener, zenPings);
        for (ZenPing zenPing : zenPings) {
            try {
                zenPing.ping(compoundPingListener, timeout);
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Ping execution rejected", ex);
                compoundPingListener.onPing(null);
            }
        }
    }

    private static class CompoundPingListener implements PingListener {

        private final PingListener listener;

        private final AtomicInteger counter;

        private PingCollection responses = new PingCollection();

        private CompoundPingListener(PingListener listener, List<? extends ZenPing> zenPings) {
            this.listener = listener;
            this.counter = new AtomicInteger(zenPings.size());
        }

        @Override
        public void onPing(PingResponse[] pings) {
            if (pings != null) {
                responses.addPings(pings);
            }
            if (counter.decrementAndGet() == 0) {
                listener.onPing(responses.toArray());
            }
        }
    }
}
