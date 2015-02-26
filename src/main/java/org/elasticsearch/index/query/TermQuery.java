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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

/**
 * An Elasticsearch TermQuery
 */
public class TermQuery extends BaseQueryBuilder implements Streamable, QueryParser {

    /**
     * This refactoring splits the parsing and the creation of the lucene query
     * This has a couple of advantages
     *  - XContent parsing creation are in one file and can be tested more easily
     *  - the class allows a typed in-memory representation of the query that can be modified before a lucene query is build
     *  - the query can be normalized and serialized via Streamable to be used as a normalized cache key (not depending on the order of the keys in the XContent)
     *  - the query can be parsed on the coordinating node to allow document prefetching etc. forwarding to the executing nodes would work via Streamable binary representation --> https://github.com/elasticsearch/elasticsearch/issues/8150
     *  - for the query cache a query tree can be "walked" to rewrite range queries into match all queries with MIN/MAX terms to get cache hits for sliding windows --> https://github.com/elasticsearch/elasticsearch/issues/9526
     *  - code wise two classes are merged into one which is nice
     *  - filter and query can maybe share once class and we add a `toFilter(QueryParserContenxt ctx)` method that returns a filter and by default return a `new QueryWrapperFilter(toQuery(context));`
     */

    public static final String NAME = "term";

    private String fieldName;

    private Object value;

    private float boost = 1.0f;

    private String queryName;

    public TermQuery() {
    }

    public TermQuery(String fieldName, Object value) {
        this.fieldName = fieldName;
        this.value = value;
    }

    public TermQuery(String fieldName, Object value, String queryName) {
        this.fieldName = fieldName;
        this.value = value;
        this.queryName = queryName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Object getValue() {
        return value;
    }

    public float getBoost() {
        return boost;
    }

    public String getQueryName() {
        return queryName;
    }

    public void setBoost(float boost) {
        this.boost = boost;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
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

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
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
    public String[] names() {
        return new String[]{TermQuery.NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        TermQuery query = new TermQuery();
        query.fromXContent(parseContext);
        return query.toQuery(parseContext);
    }

    public void fromXContent(QueryParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(context.index(), "[term] query malformed, no field");
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
                        throw new QueryParsingException(context.index(), "[term] query does not support [" + currentFieldName + "]");
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

    /**
     * Produces a lucene query from this elasticsearch query
     */
    public Query toQuery(QueryParseContext parseContext) {
        if (value == null) {
            throw new IllegalArgumentException("No value specified for term query");
        }
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
}