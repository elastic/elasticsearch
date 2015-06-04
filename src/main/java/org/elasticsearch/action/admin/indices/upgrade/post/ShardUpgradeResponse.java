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

package org.elasticsearch.action.admin.indices.upgrade.post;

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.text.ParseException;

/**
 *
 */
class ShardUpgradeResponse extends BroadcastShardResponse {

    private org.apache.lucene.util.Version version;

    private boolean primary;


    ShardUpgradeResponse() {
    }

    ShardUpgradeResponse(ShardId shardId, boolean primary, org.apache.lucene.util.Version version) {
        super(shardId);
        this.primary = primary;
        this.version = version;
    }

    public org.apache.lucene.util.Version version() {
        return this.version;
    }

    public boolean primary() {
        return primary;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        primary = in.readBoolean();
        try {
            version = org.apache.lucene.util.Version.parse(in.readString());
        } catch (ParseException ex) {
            throw new IOException("failed to parse lucene version [" + version + "]", ex);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(primary);
        out.writeString(version.toString());
    }

}