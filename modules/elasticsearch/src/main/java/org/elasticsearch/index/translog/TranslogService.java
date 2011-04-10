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

package org.elasticsearch.index.translog;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineClosedException;
import org.elasticsearch.index.engine.FlushNotAllowedEngineException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ScheduledFuture;

import static org.elasticsearch.common.unit.TimeValue.*;

/**
 * @author kimchy (shay.banon)
 */
public class TranslogService extends AbstractIndexShardComponent {

    private final ThreadPool threadPool;

    private final IndexSettingsService indexSettingsService;

    private final IndexShard indexShard;

    private final Translog translog;

    private int flushThresholdOperations;

    private ByteSizeValue flushThresholdSize;

    private TimeValue flushThresholdPeriod;

    private final TimeValue interval;

    private ScheduledFuture future;

    private final ApplySettings applySettings = new ApplySettings();

    @Inject public TranslogService(ShardId shardId, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService, ThreadPool threadPool, IndexShard indexShard, Translog translog) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.indexSettingsService = indexSettingsService;
        this.indexShard = indexShard;
        this.translog = translog;

        this.flushThresholdOperations = componentSettings.getAsInt("flush_threshold_ops", componentSettings.getAsInt("flush_threshold", 5000));
        this.flushThresholdSize = componentSettings.getAsBytesSize("flush_threshold_size", new ByteSizeValue(200, ByteSizeUnit.MB));
        this.flushThresholdPeriod = componentSettings.getAsTime("flush_threshold_period", TimeValue.timeValueMinutes(30));
        this.interval = componentSettings.getAsTime("interval", timeValueMillis(5000));

        logger.debug("interval [{}], flush_threshold_ops [{}], flush_threshold_size [{}], flush_threshold_period [{}]", interval, flushThresholdOperations, flushThresholdSize, flushThresholdPeriod);

        this.future = threadPool.schedule(interval, ThreadPool.Names.SAME, new TranslogBasedFlush());

        indexSettingsService.addListener(applySettings);
    }


    public void close() {
        indexSettingsService.removeListener(applySettings);
        this.future.cancel(true);
    }

    class ApplySettings implements IndexSettingsService.Listener {
        @Override public void onRefreshSettings(Settings settings) {
            int flushThresholdOperations = settings.getAsInt("index.translog.flush_threshold_ops", TranslogService.this.flushThresholdOperations);
            if (flushThresholdOperations != TranslogService.this.flushThresholdOperations) {
                logger.info("updating flush_threshold_ops from [{}] to [{}]", TranslogService.this.flushThresholdOperations, flushThresholdOperations);
                TranslogService.this.flushThresholdOperations = flushThresholdOperations;
            }
            ByteSizeValue flushThresholdSize = settings.getAsBytesSize("index.translog.flush_threshold_size", TranslogService.this.flushThresholdSize);
            if (!flushThresholdSize.equals(TranslogService.this.flushThresholdSize)) {
                logger.info("updating flush_threshold_size from [{}] to [{}]", TranslogService.this.flushThresholdSize, flushThresholdSize);
                TranslogService.this.flushThresholdSize = flushThresholdSize;
            }
            TimeValue flushThresholdPeriod = settings.getAsTime("index.translog.flush_threshold_period", TranslogService.this.flushThresholdPeriod);
            if (!flushThresholdPeriod.equals(TranslogService.this.flushThresholdPeriod)) {
                logger.info("updating flush_threshold_period from [{}] to [{}]", TranslogService.this.flushThresholdPeriod, flushThresholdPeriod);
                TranslogService.this.flushThresholdPeriod = flushThresholdPeriod;
            }
        }
    }

    private class TranslogBasedFlush implements Runnable {

        private volatile long lastFlushTime = System.currentTimeMillis();

        @Override public void run() {
            if (indexShard.state() == IndexShardState.CLOSED) {
                return;
            }

            int currentNumberOfOperations = translog.numberOfOperations();
            if (currentNumberOfOperations > flushThresholdOperations) {
                logger.trace("flushing translog, operations [{}], breached [{}]", currentNumberOfOperations, flushThresholdOperations);
                asyncFlushAndReschedule();
                return;
            }

            long sizeInBytes = translog.translogSizeInBytes();
            if (sizeInBytes > flushThresholdSize.bytes()) {
                logger.trace("flushing translog, size [{}], breached [{}]", new ByteSizeValue(sizeInBytes), flushThresholdSize);
                asyncFlushAndReschedule();
                return;
            }

            if ((System.currentTimeMillis() - lastFlushTime) > flushThresholdPeriod.millis()) {
                logger.trace("flushing translog, last_flush_time [{}], breached [{}]", lastFlushTime, flushThresholdPeriod);
                asyncFlushAndReschedule();
                return;
            }

            future = threadPool.schedule(interval, ThreadPool.Names.SAME, this);
        }

        private void asyncFlushAndReschedule() {
            threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new Runnable() {
                @Override public void run() {
                    try {
                        if (indexShard.state() == IndexShardState.STARTED) {
                            indexShard.flush(new Engine.Flush());
                        }
                    } catch (EngineClosedException e) {
                        // we are being closed, ignore
                    } catch (FlushNotAllowedEngineException e) {
                        // ignore this exception, we are not allowed to perform flush
                    } catch (Exception e) {
                        logger.warn("failed to flush shard on translog threshold", e);
                    }
                    lastFlushTime = System.currentTimeMillis();

                    if (indexShard.state() != IndexShardState.CLOSED) {
                        future = threadPool.schedule(interval, ThreadPool.Names.SAME, TranslogBasedFlush.this);
                    }
                }
            });
        }
    }
}
