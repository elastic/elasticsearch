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

package org.elasticsearch.discovery;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.settings.Settings;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author kimchy (shay.banon)
 */
public class DiscoveryService extends AbstractLifecycleComponent<DiscoveryService> {

    private final TimeValue initialStateTimeout;

    private final Discovery discovery;

    private volatile boolean initialStateReceived;

    @Inject public DiscoveryService(Settings settings, Discovery discovery) {
        super(settings);
        this.discovery = discovery;
        this.initialStateTimeout = componentSettings.getAsTime("initialStateTimeout", TimeValue.timeValueSeconds(30));
    }

    @Override protected void doStart() throws ElasticSearchException {
        final CountDownLatch latch = new CountDownLatch(1);
        InitialStateDiscoveryListener listener = new InitialStateDiscoveryListener() {
            @Override public void initialStateProcessed() {
                latch.countDown();
            }
        };
        discovery.addListener(listener);
        try {
            discovery.start();
            try {
                logger.trace("Waiting for {} for the initial state to be set by the discovery", initialStateTimeout);
                if (latch.await(initialStateTimeout.millis(), TimeUnit.MILLISECONDS)) {
                    logger.trace("Initial state set from discovery");
                    initialStateReceived = true;
                } else {
                    initialStateReceived = false;
                    logger.warn("Waited for {} and no initial state was set by the discovery", initialStateTimeout);
                }
            } catch (InterruptedException e) {
                // ignore
            }
        } finally {
            discovery.removeListener(listener);
        }
        logger.info(discovery.nodeDescription());
    }

    @Override protected void doStop() throws ElasticSearchException {
        discovery.stop();
    }

    @Override protected void doClose() throws ElasticSearchException {
        discovery.close();
    }

    /**
     * Returns <tt>true</tt> if the initial state was received within the timeout waiting for it
     * on {@link #doStart()}.
     */
    public boolean initialStateReceived() {
        return initialStateReceived;
    }

    public String nodeDescription() {
        return discovery.nodeDescription();
    }

    public boolean firstMaster() {
        return discovery.firstMaster();
    }

    /**
     * Publish all the changes to the cluster from the master (can be called just by the master). The publish
     * process should not publish this state to the master as well! (the master is sending it...).
     */
    public void publish(ClusterState clusterState) {
        if (!lifecycle.started()) {
            return;
        }
        discovery.publish(clusterState);
    }
}
