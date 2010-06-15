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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.util.TimeValue;

/**
 * @author kimchy (shay.banon)
 */
public class RecoveryThrottler extends AbstractComponent {

    private final Object concurrentRecoveryMutex = new Object();

    private final int concurrentRecoveries;

    private final TimeValue throttleInterval;

    private volatile int onGoingRecoveries = 0;

    private final int concurrentStreams;

    private volatile int onGoingStreams = 0;

    private final Object concurrentStreamsMutex = new Object();

    @Inject public RecoveryThrottler(Settings settings) {
        super(settings);

        concurrentRecoveries = componentSettings.getAsInt("concurrent_recoveries", Runtime.getRuntime().availableProcessors());
        concurrentStreams = componentSettings.getAsInt("concurrent_streams", Runtime.getRuntime().availableProcessors());
        throttleInterval = componentSettings.getAsTime("interval", TimeValue.timeValueMillis(100));

        logger.debug("concurrent_recoveries [{}], concurrent_streams [{}] interval [{}]", concurrentRecoveries, concurrentStreams, throttleInterval);
    }

    public boolean tryRecovery(ShardId shardId, String reason) {
        synchronized (concurrentRecoveryMutex) {
            if (onGoingRecoveries + 1 > concurrentRecoveries) {
                return false;
            }
            onGoingRecoveries++;
            logger.trace("Recovery allowed for [{}], on going [{}], allowed [{}], reason [{}]", shardId, onGoingRecoveries, concurrentRecoveries, reason);
            return true;
        }
    }

    public void recoveryDone(ShardId shardId, String reason) {
        synchronized (concurrentRecoveryMutex) {
            --onGoingRecoveries;
            logger.trace("Recovery done for [{}], on going [{}], allowed [{}], reason [{}]", shardId, onGoingRecoveries, concurrentRecoveries, reason);
        }
    }

    public int onGoingRecoveries() {
        return onGoingRecoveries;
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
