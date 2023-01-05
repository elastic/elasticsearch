package co.elastic.elasticsearch.stateless.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.Map;

public class NewCommitNotificationRequest extends ActionRequest {
    private final ShardId shardId;
    private final boolean indexingShard;
    private final long term;
    private final long generation;
    private final Map<String, StoreFileMetadata> files;

    public NewCommitNotificationRequest(
        final ShardId shardId,
        final boolean indexingShard,
        final long term,
        final long generation,
        final Map<String, StoreFileMetadata> files
    ) {
        this.shardId = shardId;
        this.indexingShard = indexingShard;
        assert term >= 0 && generation >= 0 : "term and generation should not be negative";
        this.term = term;
        this.generation = generation;
        this.files = files;
    }

    public NewCommitNotificationRequest(final StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        indexingShard = in.readBoolean();
        term = in.readVLong();
        generation = in.readVLong();
        files = in.readImmutableMap(StreamInput::readString, StoreFileMetadata::new);
    }

    public ShardId getShardId() {
        return shardId;
    }

    public boolean isIndexingShard() {
        return indexingShard;
    }

    public long getTerm() {
        return term;
    }

    public long getGeneration() {
        return generation;
    }

    public Map<String, StoreFileMetadata> getFiles() {
        return files;
    }

    public NewCommitNotificationRequest withIndexingShard(boolean indexingShard) {
        return new NewCommitNotificationRequest(shardId, indexingShard, term, generation, files);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeBoolean(indexingShard);
        out.writeVLong(term);
        out.writeVLong(generation);
        out.writeMap(files, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public String toString() {
        return "NotifyRequest{"
            + "shardId="
            + shardId
            + ", isIndexingShard="
            + indexingShard
            + ", term="
            + term
            + ", generation="
            + generation
            + ", files="
            + files
            + '}';
    }
}
