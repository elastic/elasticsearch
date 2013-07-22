package org.elasticsearch.action.percolate;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class PercolateShardRequest extends BroadcastShardOperationRequest {

    private String documentType;
    private BytesReference source;
    private BytesReference fetchedDoc;

    public PercolateShardRequest() {
    }

    public PercolateShardRequest(String index, int shardId, PercolateRequest request) {
        super(index, shardId, request);
        this.documentType = request.documentType();
        this.source = request.source();
        this.fetchedDoc = request.fetchedDoc();
    }

    public String documentType() {
        return documentType;
    }

    public BytesReference source() {
        return source;
    }

    public BytesReference fetchedDoc() {
        return fetchedDoc;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        documentType = in.readString();
        source = in.readBytesReference();
        if (in.readBoolean()) {
            fetchedDoc = in.readBytesReference();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(documentType);
        out.writeBytesReference(source);
        if (fetchedDoc != null) {
            out.writeBoolean(true);
            out.writeBytesReference(fetchedDoc);
        } else {
            out.writeBoolean(false);
        }
    }

}
