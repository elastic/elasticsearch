/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class PublishShardSizesRequest extends MasterNodeRequest<PublishShardSizesRequest> {

    private final String nodeId;
    private final Map<ShardId, ShardSize> shardSizes;

    public PublishShardSizesRequest(String nodeId, Map<ShardId, ShardSize> shardSizes) {
        this.nodeId = nodeId;
        this.shardSizes = shardSizes;
    }

    public PublishShardSizesRequest(StreamInput in) throws IOException {
        super(in);
        this.nodeId = in.readString();
        this.shardSizes = in.readImmutableMap(ShardId::new, ShardSize::from);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        out.writeMap(shardSizes);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getNodeId() {
        return nodeId;
    }

    public Map<ShardId, ShardSize> getShardSizes() {
        return shardSizes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PublishShardSizesRequest that = (PublishShardSizesRequest) o;
        return Objects.equals(nodeId, that.nodeId) && Objects.equals(shardSizes, that.shardSizes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, shardSizes);
    }

    @Override
    public String toString() {
        return "PublishShardDiskUsageRequest{nodeId='" + nodeId + ", shardSizes=" + shardSizes + '}';
    }
}
