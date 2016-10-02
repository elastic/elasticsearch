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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

/**
 * A Query builder which allows building a query given JSON string or binary data provided as input. This is useful when you want
 * to use the Java Builder API but still have JSON query strings at hand that you want to combine with other
 * query builders.
 * <p>
 * Example usage in a boolean query :
 * <pre>
 * <code>
 *      BoolQueryBuilder bool = new BoolQueryBuilder();
 *      bool.must(new WrapperQueryBuilder("{\"term\": {\"field\":\"value\"}}");
 *      bool.must(new TermQueryBuilder("field2","value2");
 * </code>
 * </pre>
 */
public class WrapperQueryBuilder extends AbstractQueryBuilder<WrapperQueryBuilder> {
    public static final String NAME = "wrapper";

    private static final ParseField QUERY_FIELD = new ParseField("query");

    private final byte[] source;

    /**
     * Creates a query builder given a query provided as a bytes array
     */
    public WrapperQueryBuilder(byte[] source) {
        if (source == null || source.length == 0) {
            throw new IllegalArgumentException("query source text cannot be null or empty");
        }
        this.source = source;
    }

    /**
     * Creates a query builder given a query provided as a string
     */
    public WrapperQueryBuilder(String source) {
        if (Strings.isEmpty(source)) {
            throw new IllegalArgumentException("query source string cannot be null or empty");
        }
        this.source = source.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Creates a query builder given a query provided as a {@link BytesReference}
     */
    public WrapperQueryBuilder(BytesReference source) {
        if (source == null || source.length() == 0) {
            throw new IllegalArgumentException("query source text cannot be null or empty");
        }
        this.source = BytesRef.deepCopyOf(source.toBytesRef()).bytes;
    }

    /**
     * Read from a stream.
     */
    public WrapperQueryBuilder(StreamInput in) throws IOException {
        super(in);
        source = in.readByteArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeByteArray(this.source);
    }

    public byte[] source() {
        return this.source;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName(), source);
        builder.endObject();
    }

    public static Optional<WrapperQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[wrapper] query malformed");
        }
        String fieldName = parser.currentName();
        if (! parseContext.getParseFieldMatcher().match(fieldName, QUERY_FIELD)) {
            throw new ParsingException(parser.getTokenLocation(), "[wrapper] query malformed, expected `query` but was " + fieldName);
        }
        parser.nextToken();

        byte[] source = parser.binaryValue();

        parser.nextToken();

        if (source == null) {
            throw new ParsingException(parser.getTokenLocation(), "wrapper query has no [query] specified");
        }
        return Optional.of(new WrapperQueryBuilder(source));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        throw new UnsupportedOperationException("this query must be rewritten first");
    }

    @Override
    protected int doHashCode() {
        return Arrays.hashCode(source);
    }

    @Override
    protected boolean doEquals(WrapperQueryBuilder other) {
        return Arrays.equals(source, other.source);   // otherwise we compare pointers
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext context) throws IOException {
        try (XContentParser qSourceParser = XContentFactory.xContent(source).createParser(source)) {
            QueryParseContext parseContext = context.newParseContext(qSourceParser);

            final QueryBuilder queryBuilder = parseContext.parseInnerQueryBuilder().orElseThrow(
                    () -> new ParsingException(qSourceParser.getTokenLocation(), "inner query cannot be empty"));
            if (boost() != DEFAULT_BOOST || queryName() != null) {
                final BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
                boolQueryBuilder.must(queryBuilder);
                return boolQueryBuilder;
            }
            return queryBuilder;
        }
    }


}
