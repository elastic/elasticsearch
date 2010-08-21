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

package org.elasticsearch.indices.recovery.throttler;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;

/**
 * Recovery Throttler allows to throttle recoveries (both gateway and peer).
 *
 * @author kimchy (shay.banon)
 */
public class RecoveryThrottler extends AbstractComponent {

    private final Object concurrentRecoveryMutex = new Object();

    private final int concurrentRecoveries;

    private final TimeValue throttleInterval;

    private volatile int onGoingGatewayRecoveries = 0;

    private volatile int onGoingPeerRecoveries = 0;

    private final int concurrentStreams;

    private volatile int onGoingStreams = 0;

    private final Object concurrentStreamsMutex = new Object();

    @Inject public RecoveryThrottler(Settings settings) {
        super(settings);

        int defaultConcurrentRecoveries = Runtime.getRuntime().availableProcessors() + 1;
        // tap it at 10 (is it a good number?)
        if (defaultConcurrentRecoveries > 10) {
            defaultConcurrentRecoveries = 10;
        } else if (defaultConcurrentRecoveries < 3) {
            defaultConcurrentRecoveries = 3;
        }

        concurrentRecoveries = componentSettings.getAsInt("concurrent_recoveries", defaultConcurrentRecoveries);
        concurrentStreams = componentSettings.getAsInt("concurrent_streams", defaultConcurrentRecoveries * 2);
        throttleInterval = componentSettings.getAsTime("interval", TimeValue.timeValueMillis(100));

        logger.debug("concurrent_recoveries [{}], concurrent_streams [{}] interval [{}]", concurrentRecoveries, concurrentStreams, throttleInterval);
    }

    /**
     * Try and check if gateway recovery is allowed. Only takes the on going gateway recoveries into account. Ignore
     * on going peer recoveries so peer recovery will not block a much more important gateway recovery.
     */
    public boolean tryGatewayRecovery(ShardId shardId, String reason) {
        synchronized (concurrentRecoveryMutex) {
            if ((onGoingGatewayRecoveries + 1) > concurrentRecoveries) {
                return false;
            }
            onGoingGatewayRecoveries++;
            logger.trace("Recovery (gateway) allowed for [{}], on_going (gateway [{}], peer [{}]), allowed [{}], reason [{}]", shardId, onGoingGatewayRecoveries, onGoingPeerRecoveries, concurrentRecoveries, reason);
            return true;
        }
    }

    /**
     * Mark gateway recvoery as done.
     */
    public void recoveryGatewayDone(ShardId shardId, String reason) {
        synchronized (concurrentRecoveryMutex) {
            --onGoingGatewayRecoveries;
            logger.trace("Recovery (gateway) done for [{}], on_going (gateway [{}], peer [{}]), allowed [{}], reason [{}]", shardId, onGoingGatewayRecoveries, onGoingPeerRecoveries, concurrentRecoveries, reason);
        }
    }

    /**
     * Try and check if peer recovery is allowed. Takes into account both on going gateway recovery and peer recovery.
     */
    public boolean tryPeerRecovery(ShardId shardId, String reason) {
        synchronized (concurrentRecoveryMutex) {
            if ((onGoingGatewayRecoveries + onGoingPeerRecoveries + 1) > concurrentRecoveries) {
                return false;
            }
            onGoingPeerRecoveries++;
            logger.trace("Recovery (peer) allowed for [{}], on_going (gateway [{}], peer [{}]), allowed [{}], reason [{}]", shardId, onGoingGatewayRecoveries, onGoingPeerRecoveries, concurrentRecoveries, reason);
            return true;
        }
    }

    /**
     * Mark peer recovery as done.
     */
    public void recoveryPeerDone(ShardId shardId, String reason) {
        synchronized (concurrentRecoveryMutex) {
            --onGoingPeerRecoveries;
            logger.trace("Recovery (peer) done for [{}], on_going (gateway [{}], peer [{}]), allowed [{}], reason [{}]", shardId, onGoingGatewayRecoveries, onGoingPeerRecoveries, concurrentRecoveries, reason);
        }
    }

    public int onGoingRecoveries() {
        return onGoingGatewayRecoveries + onGoingPeerRecoveries;
    }

    public boolean tryStream(ShardId shardId, String streamName) {
        synchronized (concurrentStreamsMutex) {
            if (onGoingStreams + 1 > concurrentStreams) {
                return false;
            }
            onGoingStreams++;
            logger.trace("Stream [{}] allowed for [{}], on going [{}], allowed [{}]", streamName, shardId, onGoingStreams, concurrentStreams);
            return true;
        }
    }

    public void streamDone(ShardId shardId, String streamName) {
        synchronized (concurrentStreamsMutex) {
            --onGoingStreams;
            logger.trace("Stream [{}] done for [{}], on going [{}], allowed [{}]", streamName, shardId, onGoingStreams, concurrentStreams);
        }
    }

    public int onGoingStreams() {
        return onGoingStreams;
    }

    public TimeValue throttleInterval() {
        return throttleInterval;
    }
}
