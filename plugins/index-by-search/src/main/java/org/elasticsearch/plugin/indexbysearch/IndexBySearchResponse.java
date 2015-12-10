package org.elasticsearch.plugin.indexbysearch;

import static org.elasticsearch.plugin.indexbysearch.BulkIndexByScrollResponse.Fields.CREATED;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

public class IndexBySearchResponse extends BulkIndexByScrollResponse {
    private long created;

    public IndexBySearchResponse() {
    }

    public IndexBySearchResponse(long took, long created, long updated, int batches, long versionConflicts, List<Failure> failures) {
        super(took, updated, batches, versionConflicts, failures);
        this.created = created;
    }

    public long created() {
        return created;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // NOCOMMIT need a round trip test for this
        super.writeTo(out);
        out.writeVLong(created);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        created = in.readVLong();
    }

    static final class Fields {
        static final XContentBuilderString CREATED = new XContentBuilderString("created");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        super.toXContent(builder, params);
        builder.field(CREATED, created);
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexBySearchResponse[");
        builder.append("took=").append(took());
        builder.append(",created=").append(created);
        builder.append(",updated=").append(updated());
        builder.append(",batches=").append(batches());
        builder.append(",versionConflicts=").append(versionConflicts());
        builder.append(",failures=").append(failures().size());
        return builder.append("]").toString();
    }

}
