package org.elasticsearch.search.sort;

import static org.elasticsearch.search.sort.SortBuilder.parseNestedFilter;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Objects;

public class NestedSortBuilder implements Writeable, Writeable.Reader<NestedSortBuilder>, ToXContentObject {
    public static final ParseField NESTED_FIELD = new ParseField("nested");
    public static final ParseField PATH_FIELD = new ParseField("path");
    public static final ParseField FILTER_FIELD = new ParseField("filter");

    private String path;
    private QueryBuilder filter;
    private NestedSortBuilder nestedSort;

    public NestedSortBuilder() {
    }

    public NestedSortBuilder(StreamInput in) throws IOException {
        read(in);
    }

    public String getPath() {
        return path;
    }

    public NestedSortBuilder setPath(final String path) {
        this.path = path;
        return this;
    }

    public QueryBuilder getFilter() {
        return filter;
    }

    public NestedSortBuilder setFilter(final QueryBuilder filter) {
        this.filter = filter;
        return this;
    }

    public NestedSortBuilder getNestedSort() {
        return nestedSort;
    }

    public NestedSortBuilder setNestedSort(final NestedSortBuilder nestedSortBuilder) {
        this.nestedSort = nestedSortBuilder;
        return this;
    }

    /**
     * Write this object's fields to a {@linkplain StreamOutput}.
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalString(path);
        out.writeOptionalNamedWriteable(filter);
        out.writeOptionalWriteable(nestedSort);
    }

    /**
     * Read {@code V}-type value from a stream.
     *
     * @param in Input to read the value from
     */
    @Override
    public NestedSortBuilder read(final StreamInput in) throws IOException {
        path = in.readOptionalString();
        filter = in.readOptionalNamedWriteable(QueryBuilder.class);
        nestedSort = in.readOptionalWriteable(NestedSortBuilder::new);
        return this;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (path != null) {
            builder.field(PATH_FIELD.getPreferredName(), path);
        }
        if (filter != null) {
            builder.field(FILTER_FIELD.getPreferredName(), filter);
        }
        if (nestedSort != null) {
            builder.field(NESTED_FIELD.getPreferredName(), nestedSort);
        }
        builder.endObject();
        return builder;
    }

    public static NestedSortBuilder fromXContent(XContentParser parser) throws IOException {
        NestedSortBuilder nestedSortBuilder = new NestedSortBuilder();
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentName = parser.currentName();
                    parser.nextToken();
                    if (currentName.equals(PATH_FIELD.getPreferredName())) {
                        nestedSortBuilder.setPath(parser.text());
                    } else if (currentName.equals(FILTER_FIELD.getPreferredName())) {
                        nestedSortBuilder.setFilter(parseNestedFilter(parser));
                    } else if (currentName.equals(NESTED_FIELD.getPreferredName())) {
                        nestedSortBuilder.setNestedSort(NestedSortBuilder.fromXContent(parser));
                    } else {
                        throw new IllegalArgumentException("malformed nested sort format, unknown field name [" + currentName + "]");
                    }
                } else {
                    throw new IllegalArgumentException("malformed nested sort format, only field names are allowed");
                }
            }
        } else {
            throw new IllegalArgumentException("malformed nested sort format, must start with an object");
        }

        return nestedSortBuilder;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NestedSortBuilder that = (NestedSortBuilder) obj;
        return Objects.equals(path, that.path)
            && Objects.equals(filter, that.filter)
            && Objects.equals(nestedSort, that.nestedSort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, filter, nestedSort);
    }
}
