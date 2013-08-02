package org.elasticsearch.action.percolate;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;

import java.io.IOException;

/**
 */
public class PercolateShardResponse extends BroadcastShardOperationResponse {

    private long count;
    private Text[] matches;

    public PercolateShardResponse() {
    }

    public PercolateShardResponse(Text[] matches, String index, int shardId) {
        super(index, shardId);
        this.matches = matches;
        this.count = matches.length;
    }

    public PercolateShardResponse(long count, String index, int shardId) {
        super(index, shardId);
        this.count = count;
        this.matches = StringText.EMPTY_ARRAY;
    }

    public Text[] matches() {
        return matches;
    }

    public long count() {
        return count;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        count = in.readVLong();
        matches = in.readTextArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(count);
        out.writeTextArray(matches);
    }
}
