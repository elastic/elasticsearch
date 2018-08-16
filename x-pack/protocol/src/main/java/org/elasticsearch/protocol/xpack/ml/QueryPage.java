package org.elasticsearch.protocol.xpack.ml;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Generic wrapper class for a page of query results and the total number of
 * query results.<br>
 * {@linkplain #count()} is the total number of results but that value may
 * not be equal to the actual length of the {@linkplain #results()} list if from
 * &amp; take or some cursor was used in the database query.
 */
public final class QueryPage<T extends ToXContent> implements ToXContentObject {

    public static final ParseField COUNT = new ParseField("count");

    private final ParseField resultsField;
    private List<T> results;
    private long count;

    public QueryPage(ParseField resultsField) {
        this.resultsField = Objects.requireNonNull(resultsField,
            "[results_field] must not be null");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(COUNT.getPreferredName(), count);
        builder.field(resultsField.getPreferredName(), results);
        builder.endObject();
        return builder;
    }

    public List<T> results() {
        return results;
    }

    void setResults(List<T> results) {
        this.results = new ArrayList<>(results);
    }

    public long count() {
        return count;
    }

    void setCount(long count) {
        this.count = count;
    }

    public ParseField getResultsField() {
        return resultsField;
    }

    @Override
    public int hashCode() {
        return Objects.hash(results, count);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        @SuppressWarnings("unchecked")
        QueryPage<T> other = (QueryPage<T>) obj;
        return Objects.equals(results, other.results) && Objects.equals(count, other.count);
    }
}
