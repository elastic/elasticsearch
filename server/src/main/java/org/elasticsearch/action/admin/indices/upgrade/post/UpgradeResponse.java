/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.post;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A response for the upgrade action.
 *
 *
 */
public class UpgradeResponse extends BroadcastResponse {

    private final Map<String, Tuple<Version, String>> versions;

    UpgradeResponse(StreamInput in) throws IOException {
        super(in);
        versions = in.readMap(StreamInput::readString, i -> Tuple.tuple(Version.readVersion(i), i.readString()));
    }

    UpgradeResponse(
        Map<String, Tuple<Version, String>> versions,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.versions = versions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(versions, StreamOutput::writeString, (o, v) -> {
            Version.writeVersion(v.v1(), o);
            o.writeString(v.v2());
        });
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
