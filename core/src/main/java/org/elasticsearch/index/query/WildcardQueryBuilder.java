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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;
import java.util.Objects;

/**
 * Implements the wildcard search query. Supported wildcards are <tt>*</tt>, which
 * matches any character sequence (including the empty one), and <tt>?</tt>,
 * which matches any single character. Note this query can be slow, as it
 * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
 * a Wildcard term should not start with one of the wildcards <tt>*</tt> or
 * <tt>?</tt>.
 */
public class WildcardQueryBuilder extends AbstractQueryBuilder<WildcardQueryBuilder> implements MultiTermQueryBuilder<WildcardQueryBuilder> {

    public static final String NAME = "wildcard";

    private final String fieldName;

    private final String value;

    private String rewrite;

    static final WildcardQueryBuilder PROTOTYPE = new WildcardQueryBuilder("field", "value");

    /**
     * Implements the wildcard search query. Supported wildcards are <tt>*</tt>, which
     * matches any character sequence (including the empty one), and <tt>?</tt>,
     * which matches any single character. Note this query can be slow, as it
     * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
     * a Wildcard term should not start with one of the wildcards <tt>*</tt> or
     * <tt>?</tt>.
     *
     * @param fieldName The field name
     * @param value The wildcard query string
     */
    public WildcardQueryBuilder(String fieldName, String value) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null.");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    public String fieldName() {
        return fieldName;
    }

    public String value() {
        return value;
    }

    public WildcardQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    public String rewrite() {
        return this.rewrite;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(WildcardQueryParser.WILDCARD_FIELD.getPreferredName(), value);
        if (rewrite != null) {
            builder.field(WildcardQueryParser.REWRITE_FIELD.getPreferredName(), rewrite);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        String indexFieldName;
        BytesRef valueBytes;

        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType != null) {
            indexFieldName = fieldType.name();
            valueBytes = fieldType.indexedValueForSearch(value);
        } else {
            indexFieldName = fieldName;
            valueBytes = new BytesRef(value);
        }

        WildcardQuery query = new WildcardQuery(new Term(indexFieldName, valueBytes));
        MultiTermQuery.RewriteMethod rewriteMethod = QueryParsers.parseRewriteMethod(context.parseFieldMatcher(), rewrite, null);
        QueryParsers.setRewriteMethod(query, rewriteMethod);
        return query;
    }

    @Override
    protected WildcardQueryBuilder doReadFrom(StreamInput in) throws IOException {
        WildcardQueryBuilder wildcardQueryBuilder = new WildcardQueryBuilder(in.readString(), in.readString());
        wildcardQueryBuilder.rewrite = in.readOptionalString();
        return wildcardQueryBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(value);
        out.writeOptionalString(rewrite);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value, rewrite);
    }

    @Override
    protected boolean doEquals(WildcardQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
                Objects.equals(value, other.value) &&
                Objects.equals(rewrite, other.rewrite);
    }
}
