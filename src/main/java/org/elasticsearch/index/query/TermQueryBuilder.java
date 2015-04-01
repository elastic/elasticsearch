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
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

/**
 * A Query that matches documents containing a term.
 *
 *
 */
public class TermQueryBuilder extends BaseQueryBuilder implements QueryParser, Streamable, BoostableQueryBuilder<TermQueryBuilder> {

    private String fieldName;

    private Object value;

    private float boost = 1.0f;  // default

    private String queryName;

    public static final String NAME = "term";

    @Inject
    public TermQueryBuilder() {
    }

    /**
     * Constructs a new term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String fieldName, String value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String fieldName, int value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String fieldName, long value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String fieldName, float value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String fieldName, double value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String fieldName, boolean value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String fieldName, Object value) {
        this.fieldName = fieldName;
        this.value = value;
    }

    /**
     * @return the fieldname for this query
     */
    public String getFieldName() {
        return this.fieldName;
    }

    /**
     * @return the value
     */
    public Object getValue() {
        return this.value;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public TermQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * @return the boost factor of this query
     */
    public float getBoost() {
        return this.boost;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public TermQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * @return the query name of this query
     */
    public String getQueryName() {
        return this.queryName;
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TermQueryBuilder.NAME);
        if (boost == 1.0f && queryName == null) {
            builder.field(fieldName, value);
        } else {
            builder.startObject(fieldName);
            builder.field("value", value);
            if (boost != 1.0f) {
                builder.field("boost", boost);
            }
            if (queryName != null) {
                builder.field("_name", queryName);
            }
            builder.endObject();
        }
        builder.endObject();
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        TermQueryBuilder query = new TermQueryBuilder();
        query.fromXContent(parseContext);
        return query.toQuery(parseContext);
    }

    public void fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "[term] query malformed, no field");
        }
        fieldName = parser.currentName();
        queryName = null;
        value = null;
        boost = 1.0f;
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if ("term".equals(currentFieldName)) {
                        value = parser.objectBytes();
                    } else if ("value".equals(currentFieldName)) {
                        value = parser.objectBytes();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[term] query does not support [" + currentFieldName + "]");
                    }
                }
            }
            parser.nextToken();
        } else {
            value = parser.text();
            // move to the next token
            parser.nextToken();
        }
    }

    public Query toQuery(QueryParseContext parseContext) {
        Preconditions.checkNotNull(fieldName, "no fieldName specified for term query");
        Preconditions.checkNotNull(value, "no value specified for term query");
        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            query = smartNameFieldMappers.mapper().termQuery(value, parseContext);
        }
        if (query == null) {
            query = new org.apache.lucene.search.TermQuery(new Term(fieldName, BytesRefs.toBytesRef(value)));
        }
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        fieldName = in.readString();
        value = in.readGenericValue();
        boost = in.readFloat();
        queryName = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(value);
        out.writeFloat(boost);
        out.writeOptionalString(queryName);
    }
}
