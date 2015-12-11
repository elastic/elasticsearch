package org.elasticsearch.plugin.indexbysearch;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;

/**
 * Request to reindex a set of documents where they are without changing their
 * locations or IDs.
 */
public class ReindexInPlaceRequest extends AbstractBulkByScrollRequest<ReindexInPlaceRequest> {
    public static final ObjectParser<ReindexInPlaceRequest, Void> PARSER = new ObjectParser<>("reindex-in-place");
    static {
        AbstractBulkByScrollRequest.setupParser(PARSER);
        PARSER.declareString(ReindexInPlaceRequest::versionType, new ParseField("version_type"));
    }

    /**
     * Should this request use the reindex version type (true, the default) or
     * the internal version type (false).
     */
    private boolean useReindexVersionType = true;

    public ReindexInPlaceRequest() {
    }

    public ReindexInPlaceRequest(SearchRequest search) {
        super(search);
    }

    /**
     * Should this request use the reindex version type (true, the default) or
     * the internal version type (false).
     */
    public boolean useReindexVersionType() {
        return useReindexVersionType;
    }

    /**
     * Should this request use the reindex version type (true, the default) or
     * the internal version type (false).
     */
    public ReindexInPlaceRequest useReindexVersionType(boolean useReindexVersionType) {
        this.useReindexVersionType = useReindexVersionType;
        return this;
    }

    /**
     * Sets useReindexVersionType using a REST-friendly string.
     */
    public void versionType(String versionType) {
        switch (versionType) {
        case "internal":
            useReindexVersionType(false);
            return;
        case "reindex":
            useReindexVersionType(true);
            return;
        default:
            throw new IllegalArgumentException("version_type may only be \"internal\" or \"reindex\" but was [" + versionType + "]");
        }
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
