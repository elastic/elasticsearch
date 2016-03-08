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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.Objects;

/**
 * A sort builder allowing to sort by score.
 */
public class ScoreSortBuilder extends SortBuilder implements NamedWriteable<ScoreSortBuilder>,
    SortElementParserTemp<ScoreSortBuilder> {

    private static final String NAME = "_score";
    static final ScoreSortBuilder PROTOTYPE = new ScoreSortBuilder();
    public static final ParseField REVERSE_FIELD = new ParseField("reverse");
    public static final ParseField ORDER_FIELD = new ParseField("order");
    private SortOrder order = SortOrder.DESC;

    /**
     * The order of sort scoring. By default, its {@link SortOrder#DESC}.
     */
    @Override
    public ScoreSortBuilder order(SortOrder order) {
        Objects.requireNonNull(order, "sort order cannot be null.");
        this.order = order;
        return this;
    }

    /**
     * Get the order of sort scoring. By default, its {@link SortOrder#DESC}.
     */
    public SortOrder order() {
        return this.order;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (order == SortOrder.ASC) {
            builder.field(REVERSE_FIELD.getPreferredName(), true);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public ScoreSortBuilder fromXContent(QueryParseContext context, String elementName) throws IOException {
        XContentParser parser = context.parser();
        ParseFieldMatcher matcher = context.parseFieldMatcher();

        XContentParser.Token token;
        String currentName = parser.currentName();
        ScoreSortBuilder result = new ScoreSortBuilder();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token.isValue()) {
                if (matcher.match(currentName, REVERSE_FIELD)) {
                    if (parser.booleanValue()) {
                        result.order(SortOrder.ASC);
                    }
                    // else we keep the default DESC
                } else if (matcher.match(currentName, ORDER_FIELD)) {
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
    public void writeTo(StreamOutput out) throws IOException {
        order.writeTo(out);
    }

    @Override
    public ScoreSortBuilder readFrom(StreamInput in) throws IOException {
        return new ScoreSortBuilder().order(SortOrder.readOrderFrom(in));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
