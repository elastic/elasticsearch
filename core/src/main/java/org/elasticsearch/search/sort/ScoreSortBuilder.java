/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.search.SortField;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Objects;

/**
 * A sort builder allowing to sort by score.
 */
public class ScoreSortBuilder extends SortBuilder<ScoreSortBuilder> {

    public static final String NAME = "_score";
    public static final ParseField ORDER_FIELD = new ParseField("order");
    private static final SortFieldAndFormat SORT_SCORE = new SortFieldAndFormat(
            new SortField(null, SortField.Type.SCORE), DocValueFormat.RAW);
    private static final SortFieldAndFormat SORT_SCORE_REVERSE = new SortFieldAndFormat(
            new SortField(null, SortField.Type.SCORE, true), DocValueFormat.RAW);

    /**
     * Build a ScoreSortBuilder default to descending sort order.
     */
    public ScoreSortBuilder() {
        // order defaults to desc when sorting on the _score
        order(SortOrder.DESC);
    }

    /**
     * Read from a stream.
     */
    public ScoreSortBuilder(StreamInput in) throws IOException {
        order(SortOrder.readFromStream(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        order.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Creates a new {@link ScoreSortBuilder} from the query held by the {@link QueryParseContext} in
     * {@link org.elasticsearch.common.xcontent.XContent} format.
     *
     * @param context the input parse context. The state on the parser contained in this context will be changed as a side effect of this
     *        method call
     * @param fieldName in some sort syntax variations the field name precedes the xContent object that specifies further parameters, e.g.
     *        in '{ "foo": { "order" : "asc"} }'. When parsing the inner object, the field name can be passed in via this argument
     */
    public static ScoreSortBuilder fromXContent(QueryParseContext context, String fieldName) throws IOException {
        XContentParser parser = context.parser();
        ParseFieldMatcher matcher = context.getParseFieldMatcher();

        XContentParser.Token token;
        String currentName = parser.currentName();
        ScoreSortBuilder result = new ScoreSortBuilder();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token.isValue()) {
                if (matcher.match(currentName, ORDER_FIELD)) {
                    result.order(SortOrder.fromString(parser.text()));
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] failed to parse field [" + currentName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unexpected token [" + token + "]");
            }
        }
        return result;
    }

    @Override
    public SortFieldAndFormat build(QueryShardContext context) {
        if (order == SortOrder.DESC) {
            return SORT_SCORE;
        } else {
            return SORT_SCORE_REVERSE;
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ScoreSortBuilder other = (ScoreSortBuilder) object;
        return Objects.equals(order, other.order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.order);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
