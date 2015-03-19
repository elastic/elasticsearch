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
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;

import java.io.IOException;

/**
 *
 */
public class FieldMaskingSpanQueryBuilder extends BaseQueryBuilder implements QueryParser, SpanQueryBuilder, BoostableQueryBuilder<FieldMaskingSpanQueryBuilder> {

    private SpanQueryBuilder queryBuilder;

    private String field;

    private float boost = -1;

    private String queryName;

    public static final String NAME = "field_masking_span";

    @Inject
    public FieldMaskingSpanQueryBuilder() {
    }

    public FieldMaskingSpanQueryBuilder(SpanQueryBuilder queryBuilder, String field) {
        this.queryBuilder = queryBuilder;
        this.field = field;
    }

    @Override
    public FieldMaskingSpanQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public FieldMaskingSpanQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FieldMaskingSpanQueryBuilder.NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        builder.field("field", field);
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        float boost = 1.0f;

        SpanQuery inner = null;
        String field = null;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    Query query = parseContext.parseInnerQuery();
                    if (!(query instanceof SpanQuery)) {
                        throw new QueryParsingException(parseContext.index(), "[field_masking_span] query] must be of type span query");
                    }
                    inner = (SpanQuery) query;
                } else {
                    throw new QueryParsingException(parseContext.index(), "[field_masking_span] query does not support [" + currentFieldName + "]");
                }
            } else {
                if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[field_masking_span] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (inner == null) {
            throw new QueryParsingException(parseContext.index(), "field_masking_span must have [query] span query clause");
        }
        if (field == null) {
            throw new QueryParsingException(parseContext.index(), "field_masking_span must have [field] set for it");
        }

        FieldMapper mapper = parseContext.fieldMapper(field);
        if (mapper != null) {
            field = mapper.names().indexName();
        }

        FieldMaskingSpanQuery query = new FieldMaskingSpanQuery(inner, field);
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
