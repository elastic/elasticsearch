package org.elasticsearch.search.reducers.metric;


import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerFactory;
import org.elasticsearch.search.reducers.metric.max.InternalMax;
import org.elasticsearch.search.reducers.metric.max.MaxReducer;

import java.io.IOException;

public abstract class SimpleMetricReducerParser implements Reducer.Parser {

    public static final ParseField BUCKETS_FIELD = new ParseField("buckets");
    public static final ParseField FIELD_NAME_FIELD = new ParseField("field");

    @Override
    public String type() {
        return InternalMax.TYPE.name();
    }

    @Override
    public ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException {
        String buckets = null;
        String fieldName = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (BUCKETS_FIELD.match(currentFieldName)) {
                    buckets = parser.text();
                } else if (FIELD_NAME_FIELD.match(currentFieldName)) {
                    fieldName = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName
                        + "].");
            }
        }

        if (buckets == null) {
            throw new SearchParseException(context, "Missing [" + BUCKETS_FIELD.getPreferredName() + "] in " + type() + " reducer [" + reducerName + "]");
        }

        if (fieldName == null) {
            throw new SearchParseException(context, "Missing [" + FIELD_NAME_FIELD.getPreferredName() + "] in " + type() + " reducer [" + reducerName + "]");
        }
        return new MaxReducer.Factory(reducerName, buckets, fieldName);
    }

}

