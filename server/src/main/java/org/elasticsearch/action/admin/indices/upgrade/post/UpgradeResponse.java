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
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A response for the upgrade action.
 *
 *
 */
public class UpgradeResponse extends BroadcastResponse {

    private Map<String, Tuple<Version, String>> versions;

    UpgradeResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        versions = new HashMap<>();
        for (int i=0; i<size; i++) {
            String index = in.readString();
            Version upgradeVersion = Version.readVersion(in);
            String oldestLuceneSegment = in.readString();
            versions.put(index, new Tuple<>(upgradeVersion, oldestLuceneSegment));
        }
    }

    UpgradeResponse(Map<String, Tuple<Version, String>> versions, int totalShards, int successfulShards, int failedShards,
                    List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.versions = versions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(versions.size());
        for(Map.Entry<String, Tuple<Version, String>> entry : versions.entrySet()) {
            out.writeString(entry.getKey());
            Version.writeVersion(entry.getValue().v1(), out);
            out.writeString(entry.getValue().v2());
        }
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("upgraded_indices");
        for (Map.Entry<String, Tuple<Version, String>> entry : versions.entrySet()) {
            builder.startObject(entry.getKey());
            builder.field("upgrade_version", entry.getValue().v1());
            builder.field("oldest_lucene_segment_version", entry.getValue().v2());
            builder.endObject();
        }
        builder.endObject();
    }

    /**
     * Returns the highest upgrade version of the node that performed metadata upgrade and the
     * the version of the oldest lucene segment for each index that was upgraded.
     */
    public Map<String, Tuple<Version, String>> versions() {
        return versions;
    }
}
