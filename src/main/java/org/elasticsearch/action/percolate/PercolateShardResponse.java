package org.elasticsearch.action.percolate;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.percolator.PercolatorService;

import java.io.IOException;

/**
 */
public class PercolateShardResponse extends BroadcastShardOperationResponse {

    private long count;
    private Text[] matches;

    // Request fields:
    private boolean limit;
    private int requestedSize;

    public PercolateShardResponse() {
    }

    public PercolateShardResponse(Text[] matches, long count, PercolatorService.PercolateContext context, String index, int shardId) {
        super(index, shardId);
        this.matches = matches;
        this.count = count;
        this.limit = context.limit;
        this.requestedSize = context.size;
    }

    public PercolateShardResponse(long count, PercolatorService.PercolateContext context, String index, int shardId) {
        super(index, shardId);
        this.count = count;
        this.matches = StringText.EMPTY_ARRAY;
        this.limit = context.limit;
        this.requestedSize = context.size;
    }

    public PercolateShardResponse(String index, int shardId) {
        super(index, shardId);
        this.matches = StringText.EMPTY_ARRAY;
    }

    public Text[] matches() {
        return matches;
    }

    public long count() {
        return count;
    }

    public boolean limit() {
        return limit;
    }

    public int requestedSize() {
        return requestedSize;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        count = in.readVLong();
        matches = in.readTextArray();

        limit = in.readBoolean();
        requestedSize = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(count);
        out.writeTextArray(matches);

        out.writeBoolean(limit);
        out.writeVLong(requestedSize);
    }
}
