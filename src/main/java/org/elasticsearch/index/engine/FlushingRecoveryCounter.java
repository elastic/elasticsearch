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

package org.elasticsearch.index.engine;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.store.Store;

/**
 * A special {@link RecoveryCounter} that flushes the engine when all
 * recoveries have completed
 */
public final class FlushingRecoveryCounter extends RecoveryCounter {

    private final Engine engine;
    private final ESLogger logger;

    FlushingRecoveryCounter(Engine engine, Store store, ESLogger logger) {
        super(store);
        this.engine = engine;
        this.logger = logger;
    }

    @Override
    int endRecovery() throws ElasticsearchException {
        int left = super.endRecovery();
        if (left == 0) {
            try {
                engine.flush();
            } catch (IllegalIndexShardStateException|FlushNotAllowedEngineException e) {
                // we are being closed, or in created state, ignore
                // OR, we are not allowed to perform flush, ignore
            } catch (Throwable e) {
                logger.warn("failed to flush shard post recovery", e);
            }
        }
        return left;
    }
}