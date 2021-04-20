/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
