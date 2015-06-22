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

import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Objects;

public class FieldMaskingSpanQueryBuilder extends AbstractQueryBuilder<FieldMaskingSpanQueryBuilder> implements SpanQueryBuilder<FieldMaskingSpanQueryBuilder>, BoostableQueryBuilder<FieldMaskingSpanQueryBuilder> {

    public static final String NAME = "field_masking_span";

    private final SpanQueryBuilder queryBuilder;

    private final String fieldName;

    private float boost = 1.0f;

    private String queryName;

    static final FieldMaskingSpanQueryBuilder PROTOTYPE = new FieldMaskingSpanQueryBuilder(null, null);

    /**
     * Constructs a new {@link FieldMaskingSpanQueryBuilder} given an inner {@link SpanQueryBuilder} for
     * a given field
     * @param queryBuilder inner {@link SpanQueryBuilder}
     * @param fieldName the field name
     */
    public FieldMaskingSpanQueryBuilder(SpanQueryBuilder queryBuilder, String fieldName) {
        this.queryBuilder = queryBuilder;
        this.fieldName = fieldName;
    }

    /**
     * @return the field name for this query
     */
    public String fieldName() {
        return this.fieldName;
    }

    /**
     * @return the inner {@link QueryBuilder}
     */
    public SpanQueryBuilder innerQuery() {
        return this.queryBuilder;
    }

    @Override
    public FieldMaskingSpanQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * @return the boost factor for this query
     */
    public float boost() {
        return this.boost;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public FieldMaskingSpanQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * @return the query name for this query
     */
    public String queryName() {
        return this.queryName;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        builder.field("field", fieldName);
        builder.field("boost", boost);
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    @Override
    public SpanQuery toQuery(QueryParseContext parseContext) throws QueryParsingException, IOException {
        String fieldInQuery = this.fieldName;
        MappedFieldType fieldType = parseContext.fieldMapper(fieldName);
        if (fieldType != null) {
            fieldInQuery = fieldType.names().indexName();
        }
        SpanQuery innerQuery = this.queryBuilder.toQuery(parseContext);

        FieldMaskingSpanQuery query = new FieldMaskingSpanQuery(innerQuery, fieldInQuery);
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationExceptions = null;
        if (queryBuilder == null) {
            validationExceptions = QueryValidationException.addValidationError("[field_masking_span] must have inner span query clause", validationExceptions);
        }
        if (fieldName == null || fieldName.isEmpty()) {
            validationExceptions = QueryValidationException.addValidationError("[field_masking_span] must have set field name", validationExceptions);
        }
        return validationExceptions;
    }

    @Override
    public FieldMaskingSpanQueryBuilder readFrom(StreamInput in) throws IOException {
        QueryBuilder innerQueryBuilder = in.readNamedWriteable();
        FieldMaskingSpanQueryBuilder queryBuilder = new FieldMaskingSpanQueryBuilder((SpanQueryBuilder) innerQueryBuilder, in.readString());
        queryBuilder.queryName = in.readOptionalString();
        queryBuilder.boost = in.readFloat();
        return queryBuilder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(this.queryBuilder);
        out.writeString(this.fieldName);
        out.writeOptionalString(queryName);
        out.writeFloat(boost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryBuilder, fieldName, boost, queryName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FieldMaskingSpanQueryBuilder other = (FieldMaskingSpanQueryBuilder) obj;
        return Objects.equals(queryBuilder, other.queryBuilder) &&
               Objects.equals(fieldName, other.fieldName) &&
               Objects.equals(boost, other.boost) &&
               Objects.equals(queryName, other.queryName);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
