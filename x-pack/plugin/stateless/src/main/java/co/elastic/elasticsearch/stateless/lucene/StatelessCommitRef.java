package co.elastic.elasticsearch.stateless.lucene;

import org.elasticsearch.common.lucene.FilterIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class StatelessCommitRef extends FilterIndexCommit implements Closeable {

    private final ShardId shardId;
    private final Engine.IndexCommitRef indexCommitRef;
    private final Set<String> additionalFiles;
    private final AtomicBoolean released;
    private final long primaryTerm;

    public StatelessCommitRef(ShardId shardId, Engine.IndexCommitRef indexCommitRef, Set<String> additionalFiles, long primaryTerm) {
        super(indexCommitRef.getIndexCommit());
        this.shardId = Objects.requireNonNull(shardId);
        this.indexCommitRef = indexCommitRef;
        this.additionalFiles = Objects.requireNonNull(additionalFiles);
        this.primaryTerm = primaryTerm;
        this.released = new AtomicBoolean();
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public Set<String> getAdditionalFiles() {
        return additionalFiles;
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
        return "StatelessCommitRef(" + shardId + ',' + primaryTerm + "," + in.toString() + ')';
    }
}
