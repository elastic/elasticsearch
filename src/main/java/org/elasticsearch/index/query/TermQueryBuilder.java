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
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.inject.Inject;
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
public class TermQueryBuilder extends BaseQueryBuilder implements QueryParser, BoostableQueryBuilder<TermQueryBuilder> {

    private final String name;

    private final Object value;

    private float boost = -1;

    private String queryName;
    
    public static final String NAME = "term";

    @Inject
    public TermQueryBuilder() {
        this(null, null);
    }

    /**
     * Constructs a new term query.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String name, String value) {
        this(name, (Object) value);
    }

    /**
     * Constructs a new term query.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String name, int value) {
        this(name, (Object) value);
    }

    /**
     * Constructs a new term query.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String name, long value) {
        this(name, (Object) value);
    }

    /**
     * Constructs a new term query.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String name, float value) {
        this(name, (Object) value);
    }

    /**
     * Constructs a new term query.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String name, double value) {
        this(name, (Object) value);
    }

    /**
     * Constructs a new term query.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String name, boolean value) {
        this(name, (Object) value);
    }

    /**
     * Constructs a new term query.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public TermQueryBuilder(String name, Object value) {
        this.name = name;
        this.value = value;
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
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public TermQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }
    
    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TermQueryBuilder.NAME);
        if (boost == -1 && queryName == null) {
            builder.field(name, value);
        } else {
            builder.startObject(name);
            builder.field("value", value);
            if (boost != -1) {
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
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "[term] query malformed, no field");
        }
        String fieldName = parser.currentName();

        String queryName = null;
        Object value = null;
        float boost = 1.0f;
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

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No value specified for term query");
        }

        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            query = smartNameFieldMappers.mapper().termQuery(value, parseContext);
        }
        if (query == null) {
            query = new TermQuery(new Term(fieldName, BytesRefs.toBytesRef(value)));
        }
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
