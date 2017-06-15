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

package org.elasticsearch.index.shard;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Represents the sequence number component of the primary context. This is the knowledge on the primary of the in-sync and initializing
 * shards and their local checkpoints.
 */
public class PrimaryContext implements Writeable {

    private long clusterStateVersion;

    public long clusterStateVersion() {
        return clusterStateVersion;
    }

    private ObjectLongMap<String> inSyncLocalCheckpoints;

    public ObjectLongMap<String> inSyncLocalCheckpoints() {
        return inSyncLocalCheckpoints;
    }

    private ObjectLongMap<String> trackingLocalCheckpoints;

    public ObjectLongMap<String> trackingLocalCheckpoints() {
        return trackingLocalCheckpoints;
    }

    public PrimaryContext(
            final long clusterStateVersion,
            final ObjectLongMap<String> inSyncLocalCheckpoints,
            final ObjectLongMap<String> trackingLocalCheckpoints) {
        this.clusterStateVersion = clusterStateVersion;
        this.inSyncLocalCheckpoints = inSyncLocalCheckpoints;
        this.trackingLocalCheckpoints = trackingLocalCheckpoints;
    }

    public PrimaryContext(final StreamInput in) throws IOException {
        clusterStateVersion = in.readVLong();
        inSyncLocalCheckpoints = readMap(in);
        trackingLocalCheckpoints = readMap(in);
    }

    private static ObjectLongMap<String> readMap(final StreamInput in) throws IOException {
        final int length = in.readInt();
        final ObjectLongMap<String> map = new ObjectLongHashMap<>(length);
        for (int i = 0; i < length; i++) {
            final String key = in.readString();
            final long value = in.readZLong();
            map.addTo(key, value);
        }
        return map;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVLong(clusterStateVersion);
        writeMap(out, inSyncLocalCheckpoints);
        writeMap(out, trackingLocalCheckpoints);
    }

    private static void writeMap(final StreamOutput out, final ObjectLongMap<String> map) throws IOException {
        out.writeInt(map.size());
        for (ObjectLongCursor<String> cursor : map) {
            out.writeString(cursor.key);
            out.writeZLong(cursor.value);
        }
    }

    @Override
    public String toString() {
        return "PrimaryContext{" +
                "clusterStateVersion=" + clusterStateVersion +
                ", inSyncLocalCheckpoints=" + inSyncLocalCheckpoints +
                ", trackingLocalCheckpoints=" + trackingLocalCheckpoints +
                '}';
    }

}
