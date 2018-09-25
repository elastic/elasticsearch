package org.elasticsearch.client.rollup;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;


public class DeleteRollupJobRequest implements Validatable, ToXContentObject {

    private final String id;

    private static final ParseField ID_FIELD = new ParseField("id");

    public DeleteRollupJobRequest(String id) {
        this.id = Objects.requireNonNull(id,"id parameter must not be null");
    }

    public String getId() {
        return id;
    }

    private static final ConstructingObjectParser<DeleteRollupJobRequest, Void> PARSER =
        new ConstructingObjectParser<>("request",  a -> {
            return new DeleteRollupJobRequest((String) a[0]);
        });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID_FIELD);
    }

    public static DeleteRollupJobRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID_FIELD.getPreferredName(), this.id);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteRollupJobRequest that = (DeleteRollupJobRequest) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
