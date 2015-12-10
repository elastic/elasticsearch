package org.elasticsearch.plugin.indexbysearch;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class ReindexInPlaceRequest extends AbstractBulkByScrollRequest<ReindexInPlaceRequest> {
    private boolean useReindexVersionType;

    public ReindexInPlaceRequest() {
    }

    public ReindexInPlaceRequest(SearchRequest search) {
        super(search);
    }

    public boolean useReindexVersionType() {
        return useReindexVersionType;
    }

    public ReindexInPlaceRequest useReindexVersionType(boolean useReindexVersionType) {
        this.useReindexVersionType = useReindexVersionType;
        return this;
    }

    @Override
    protected ReindexInPlaceRequest self() {
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        useReindexVersionType = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(useReindexVersionType);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("reindex ");
        searchToString(b);
        return b.toString();
    }
}
