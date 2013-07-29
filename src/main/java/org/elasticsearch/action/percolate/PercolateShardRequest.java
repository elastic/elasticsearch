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
    private BytesReference docSource;

    public PercolateShardRequest() {
    }

    public PercolateShardRequest(String index, int shardId, PercolateRequest request) {
        super(index, shardId, request);
        this.documentType = request.documentType();
        this.source = request.source();
        this.docSource = request.docSource();
    }

    public String documentType() {
        return documentType;
    }

    public BytesReference source() {
        return source;
    }

    public BytesReference docSource() {
        return docSource;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        documentType = in.readString();
        source = in.readBytesReference();
        docSource = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(documentType);
        out.writeBytesReference(source);
        out.writeBytesReference(docSource);
    }

}
