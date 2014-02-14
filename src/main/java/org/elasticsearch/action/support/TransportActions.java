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

package org.elasticsearch.action.support;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.indices.IndexMissingException;

/**
 */
public class TransportActions {

    public static boolean isShardNotAvailableException(Throwable t) {
        Throwable actual = ExceptionsHelper.unwrapCause(t);
        if (actual instanceof IllegalIndexShardStateException) {
            return true;
        }
        if (actual instanceof IndexMissingException) {
            return true;
        }
        if (actual instanceof IndexShardMissingException) {
            return true;
        }
        if (actual instanceof NoShardAvailableActionException) {
            return true;
        }
        return false;
    }

    /**
     * If a failure is already present, should this failure override it or not for read operations.
     */
    public static boolean isReadOverrideException(Throwable t) {
        if (isShardNotAvailableException(t)) {
            return false;
        }
        return true;
    }
}
