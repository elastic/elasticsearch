package org.elasticsearch.index.percolator;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.shard.ShardId;

/**
 * Exception during percolating document(s) at runtime.
 */
public class PercolateException extends ElasticSearchException {

    private final ShardId shardId;

    public PercolateException(String msg, ShardId shardId) {
        super(msg);
        this.shardId = shardId;
    }

    public PercolateException(ShardId shardId, String msg, Throwable cause) {
        super(msg, cause);
        this.shardId = shardId;
    }

    public ShardId getShardId() {
        return shardId;
    }
}
