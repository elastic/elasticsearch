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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.IndexSettings;

/**
 * Interface for responses that modify data in some shard like delete, index, and shardBulk.
 */
public interface ReplicatedWriteResponse {
    /**
     * Mark the request with if it was forced to refresh the index. All implementations by default assume that the request didn't force a
     * refresh unless set otherwise so it mostly only makes sense to call this with {@code true}. Requests that set
     * {@link WriteRequest#setRefresh(boolean)} to true should always set this to true. Requests that set
     * {@link WriteRequest#setBlockUntilRefresh(boolean)} to true should only set this to true if they run out of refresh
     * listener slots (see {@link IndexSettings#MAX_REFRESH_LISTENERS_PER_SHARD}).
     */
    public abstract void setForcedRefresh(boolean forcedRefresh);
}
