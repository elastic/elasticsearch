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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.text.ParseException;

class ShardUpgradeResult implements Writeable {

    private ShardId shardId;

    private org.apache.lucene.util.Version oldestLuceneSegment;

    private Version upgradeVersion;

    private boolean primary;

    ShardUpgradeResult(ShardId shardId, boolean primary, Version upgradeVersion, org.apache.lucene.util.Version oldestLuceneSegment) {
        this.shardId = shardId;
        this.primary = primary;
        this.upgradeVersion = upgradeVersion;
        this.oldestLuceneSegment = oldestLuceneSegment;
    }

    ShardUpgradeResult(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        primary = in.readBoolean();
        upgradeVersion = Version.readVersion(in);
        try {
            oldestLuceneSegment = org.apache.lucene.util.Version.parse(in.readString());
        } catch (ParseException ex) {
            throw new IOException("failed to parse lucene version [" + oldestLuceneSegment + "]", ex);
        }
    }

    public ShardId getShardId() {
        return shardId;
    }

    public org.apache.lucene.util.Version oldestLuceneSegment() {
        return this.oldestLuceneSegment;
    }

    public Version upgradeVersion() {
        return this.upgradeVersion;
    }

    public boolean primary() {
        return primary;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeBoolean(primary);
        Version.writeVersion(upgradeVersion, out);
        out.writeString(oldestLuceneSegment.toString());
    }
}
