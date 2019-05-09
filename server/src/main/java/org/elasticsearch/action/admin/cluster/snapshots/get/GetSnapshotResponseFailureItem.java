package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetSnapshotResponseFailureItem implements Streamable, ToXContentObject, Comparable<GetSnapshotResponseFailureItem> {
    static final ConstructingObjectParser<GetSnapshotResponseFailureItem, Void> PARSER =
            new ConstructingObjectParser<>(GetSnapshotResponseFailureItem.class.getName(), true,
                    (args) -> new GetSnapshotResponseFailureItem((String) args[0], (ElasticsearchException) args[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("repository"));
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> ElasticsearchException.fromXContent(p),
                new ParseField("error"));
    }

    private String repository;
    private Exception error;


    public String getRepository() {
        return repository;
    }

    public Exception getError() {
        return error;
    }


    GetSnapshotResponseFailureItem(String repository, ElasticsearchException error) {
        this.repository = repository;
        this.error = error;
    }

    public GetSnapshotResponseFailureItem(StreamInput in) throws IOException {
        readFrom(in);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        repository = in.readString();
        error = in.readException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeException(error);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("repository", repository);
        ElasticsearchException.generateFailureXContent(builder, params, error, true);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetSnapshotResponseFailureItem that = (GetSnapshotResponseFailureItem) o;
        return Objects.equals(repository, that.repository) && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(repository, error);
    }

    @Override
    public int compareTo(GetSnapshotResponseFailureItem o) {
        return repository.compareTo(o.repository);
    }
}
