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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;

import java.util.Objects;

public interface MappingUpdatePerformer {
    /**
     * Determine if any mappings need to be updated, and update them on the
     * master node if necessary. Returnes a failed {@code Engine.IndexResult}
     * in the event updating the mappings fails or null if successful.
     * Throws a {@code ReplicationOperation.RetryOnPrimaryException} if the
     * operation needs to be retried on the primary due to the mappings not
     * being present yet, or a different exception if updating the mappings
     * on the master failed.
     */
    @Nullable
    MappingUpdateResult updateMappingsIfNeeded(IndexShard primary, IndexRequest request) throws Exception;

    /**
     * Class encapsulating the resulting of potentially updating the mapping
     */
    class MappingUpdateResult {
        @Nullable
        public final Engine.Index operation;
        @Nullable
        public final Exception failure;

        MappingUpdateResult(Exception failure) {
            Objects.requireNonNull(failure, "failure cannot be null");
            this.failure = failure;
            this.operation = null;
        }

        MappingUpdateResult(Engine.Index operation) {
            Objects.requireNonNull(operation, "operation cannot be null");
            this.operation = operation;
            this.failure = null;
        }

        public boolean isFailed() {
            return failure != null;
        }
    }
}
