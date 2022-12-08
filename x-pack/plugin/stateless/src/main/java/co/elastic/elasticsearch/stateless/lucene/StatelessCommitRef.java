package co.elastic.elasticsearch.stateless.lucene;

import org.elasticsearch.common.lucene.FilterIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class StatelessCommitRef extends FilterIndexCommit implements Closeable {

    private final ShardId shardId;
    private final Engine.IndexCommitRef indexCommitRef;
    private final AtomicBoolean released;

    public StatelessCommitRef(ShardId shardId, Engine.IndexCommitRef indexCommitRef) {
        super(indexCommitRef.getIndexCommit());
        this.shardId = Objects.requireNonNull(shardId);
        this.indexCommitRef = indexCommitRef;
        this.released = new AtomicBoolean();
    }

    @Override
    public void close() throws IOException {
        if (released.compareAndSet(false, true)) {
            indexCommitRef.close();
        }
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public String toString() {
        return "StatelessCommitRef(" + shardId + "," + in.toString() + ')';
    }
}
