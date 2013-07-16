package org.elasticsearch.action.percolate;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;

import java.io.IOException;

/**
 */
public class PercolateShardResponse extends BroadcastShardOperationResponse {

    private Text[] matches;

    public PercolateShardResponse() {
    }

    public PercolateShardResponse(Text[] matches, String index, int shardId) {
        super(index, shardId);
        this.matches = matches;
    }

    public Text[] matches() {
        return matches;
    }

    public void matches(Text[] matches) {
        this.matches = matches;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        matches = in.readTextArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeTextArray(matches);
    }
}
