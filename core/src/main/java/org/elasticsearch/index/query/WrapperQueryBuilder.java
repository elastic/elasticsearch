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
    private final byte[] source;
    static final WrapperQueryBuilder PROTOTYPE = new WrapperQueryBuilder((byte[]) new byte[]{0});

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
        this.source = source.array();
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
        builder.field(WrapperQueryParser.QUERY_FIELD.getPreferredName(), source);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        try (XContentParser qSourceParser = XContentFactory.xContent(source).createParser(source)) {
            final QueryShardContext contextCopy = new QueryShardContext(context);
            contextCopy.reset(qSourceParser);
            contextCopy.parseFieldMatcher(context.parseFieldMatcher());
            QueryBuilder<?> result = contextCopy.parseContext().parseInnerQueryBuilder();
            context.combineNamedQueries(contextCopy);
            return result.toQuery(context);
        }
    }

    @Override
    protected WrapperQueryBuilder doReadFrom(StreamInput in) throws IOException {
        return new WrapperQueryBuilder(in.readByteArray());
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeByteArray(this.source);
    }

    @Override
    protected int doHashCode() {
        return Arrays.hashCode(source);
    }

    @Override
    protected boolean doEquals(WrapperQueryBuilder other) {
        return Arrays.equals(source, other.source);   // otherwise we compare pointers
    }
}
