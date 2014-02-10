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

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.recovery.RecoveryMetrics;

import java.io.IOException;

/**
 *
 */
public class ShardRecoveryResponse extends BroadcastShardOperationResponse implements ToXContent {

    boolean recovering = false;
    IndexShardState state;
    RecoveryMetrics recoveryMetrics;

    public ShardRecoveryResponse() { }

    public ShardRecoveryResponse(String index, int shardId) {
        super(index, shardId);
    }

    public boolean recovering() {
        return recovering;
    }

    public void recovering(boolean recovering) {
        this.recovering = recovering;
    }

    public void recoveryMetrics(RecoveryMetrics recoveryMetrics) {
        this.recoveryMetrics = recoveryMetrics;
    }

    public RecoveryMetrics recoveryMetrics() {
        return recoveryMetrics;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        recoveryMetrics.toXContent(builder, params);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(recovering);
        out.writeByte(state.id());
        out.writeOptionalStreamable(recoveryMetrics);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        recovering = in.readBoolean();
        state = IndexShardState.fromId(in.readByte());
        recoveryMetrics = RecoveryMetrics.readOptionalRecoveryMetricsFrom(in);
    }
}