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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Objects;

/**
 * A Query that matches documents containing a term.
 */
public class TermQueryBuilder extends QueryBuilder implements Streamable, BoostableQueryBuilder<TermQueryBuilder> {

    private String fieldName;

    private Object value;

    private float boost = 1.0f;

    private String queryName;

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
        if (value instanceof String) {
            this.value = BytesRefs.toBytesRef(value);
        } else {
            this.value = value;
        }
    }

    public TermQueryBuilder() {
        // for serialization only
    }

    /**
     * @return the field name used in this query
     */
    public String fieldName() {
        return this.fieldName;
    }

    /**
     * @return the value used in this query
     */
    public Object value() {
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
     * Gets the boost for this query.
     */
    public float boost() {
        return this.boost;
    }

    /**
     * Sets the query name for the query.
     */
    public TermQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * Gets the query name for the query.
     */
    public String queryName() {
        return this.queryName;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TermQueryParser.NAME);
        Object valueToWrite = this.value;
        if (valueToWrite instanceof BytesRef) {
            valueToWrite = ((BytesRef) valueToWrite).utf8ToString();
        }
        if (boost == 1.0f && queryName == null) {
            builder.field(fieldName, valueToWrite);
        } else {
            builder.startObject(fieldName);
            builder.field("value", valueToWrite);
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
    public Query toQuery(QueryParseContext parseContext) {
        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(this.fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            query = smartNameFieldMappers.mapper().termQuery(this.value, parseContext);
        }
        if (query == null) {
            query = new TermQuery(new Term(this.fieldName, BytesRefs.toBytesRef(this.value)));
        }
        query.setBoost(this.boost);
        if (this.queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (this.fieldName == null || this.fieldName.isEmpty()) {
            validationException = QueryValidationException.addValidationError("field name cannot be null or empty.", validationException);
        }
        if (this.value == null) {
            validationException = QueryValidationException.addValidationError("value cannot be null.", validationException);
        }
        return validationException;
    }

    @Override
    protected String parserName() {
        return TermQueryParser.NAME;
    }

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
    public int hashCode() {
        return Objects.hash(fieldName, value, boost, queryName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TermQueryBuilder other = (TermQueryBuilder) obj;
        return Objects.equals(fieldName, other.fieldName) &&
               Objects.equals(value, other.value) &&
               Objects.equals(boost, other.boost) &&
               Objects.equals(queryName, other.queryName);
    }
}
