/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.rest.RestStatus;

/**
 *
 */
public class SearchPhaseExecutionException extends ElasticSearchException {

    private final String phaseName;

    private ShardSearchFailure[] shardFailures;

    public SearchPhaseExecutionException(String phaseName, String msg, ShardSearchFailure[] shardFailures) {
        super(buildMessage(phaseName, msg, shardFailures));
        this.phaseName = phaseName;
        this.shardFailures = shardFailures;
    }

    public SearchPhaseExecutionException(String phaseName, String msg, Throwable cause, ShardSearchFailure[] shardFailures) {
        super(buildMessage(phaseName, msg, shardFailures), cause);
        this.phaseName = phaseName;
        this.shardFailures = shardFailures;
    }

    @Override
    public RestStatus status() {
        if (shardFailures.length == 0) {
            // if no successful shards, it means no active shards, so just return SERVICE_UNAVAILABLE
            return RestStatus.SERVICE_UNAVAILABLE;
        }
        RestStatus status = shardFailures[0].status();
        if (shardFailures.length > 1) {
            for (int i = 1; i < shardFailures.length; i++) {
                if (shardFailures[i].status().getStatus() >= 500) {
                    status = shardFailures[i].status();
                }
            }
        }
        return status;
    }

    public String phaseName() {
        return phaseName;
    }

    public ShardSearchFailure[] shardFailures() {
        return shardFailures;
    }

    private static String buildMessage(String phaseName, String msg, ShardSearchFailure[] shardFailures) {
        StringBuilder sb = new StringBuilder();
        sb.append("Failed to execute phase [").append(phaseName).append("], ").append(msg);
        if (shardFailures != null && shardFailures.length > 0) {
            sb.append("; shardFailures ");
            for (ShardSearchFailure shardFailure : shardFailures) {
                if (shardFailure.shard() != null) {
                    sb.append("{").append(shardFailure.shard()).append(": ").append(shardFailure.reason()).append("}");
                } else {
                    sb.append("{").append(shardFailure.reason()).append("}");
                }
            }
        }
        return sb.toString();
    }
}
