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

import org.elasticsearch.common.util.concurrent.ReleasableLock;

/**
 * SharedFSEngine behaves similarly to InternalEngine, however, during
 * recovery, it does not take a snapshot of the translog or index and it
 * performs stage1 (file transfer) under the write lock.
 */
public class SharedFSEngine extends InternalEngine {
    public SharedFSEngine(EngineConfig engineConfig) throws EngineException {
        super(engineConfig);
    }

    @Override
    public void recover(RecoveryHandler recoveryHandler) throws EngineException {
        store.incRef();
        try  {
            logger.trace("[pre-recovery] acquiring write lock");
            try (ReleasableLock lock = writeLock.acquire()) {
                // phase1 under lock
                ensureOpen();
                try {
                    logger.trace("[phase1] performing phase 1 recovery (file recovery)");
                    recoveryHandler.phase1(null);
                } catch (Throwable e) {
                    maybeFailEngine("recovery phase 1 (file transfer)", e);
                    throw new RecoveryEngineException(shardId, 1, "Execution failed", wrapIfClosed(e));
                }
            }
            try {
                logger.trace("[phase2] performing phase 2 recovery (translog replay)");
                recoveryHandler.phase2(null);
            } catch (Throwable e) {
                maybeFailEngine("recovery phase 2 (snapshot transfer)", e);
                throw new RecoveryEngineException(shardId, 2, "Execution failed", wrapIfClosed(e));
            }
            try {
                logger.trace("[phase3] performing phase 3 recovery (finalization)");
                recoveryHandler.phase3(null);
            } catch (Throwable e) {
                maybeFailEngine("recovery phase 3 (finalization)", e);
                throw new RecoveryEngineException(shardId, 3, "Execution failed", wrapIfClosed(e));
            }
        } finally {
            store.decRef();
        }
        logger.trace("[post-recovery] recovery complete");
    }
}
