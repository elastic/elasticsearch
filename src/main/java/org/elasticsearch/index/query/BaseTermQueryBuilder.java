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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public abstract class BaseTermQueryBuilder<QB extends BoostableQueryBuilder<QB>> extends QueryBuilder implements Streamable, BoostableQueryBuilder<QB> {
    
    /** Name of field to match against. */
    protected String fieldName;

    /** Value to find matches for. */
    protected Object value;

    /** Query boost. */
    protected float boost = 1.0f;

    /** Name of the query. */
    protected String queryName;

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, String value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, int value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, long value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, float value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, double value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, boolean value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, Object value) {
        this.fieldName = fieldName;
        if (value instanceof String) {
            this.value = BytesRefs.toBytesRef(value);
        } else {
            this.value = value;
        }
    }

    BaseTermQueryBuilder() {
        // for serialization only
    }

    /** Returns the field name used in this query. */
    public String fieldName() {
        return this.fieldName;
    }

    /** Returns the value used in this query. */
    public Object value() {
        return this.value;
    }
    
    /** Returns the query name for the query. */
    public String queryName() {
        return this.queryName;
    }
    /**
     * Sets the query name for the query.
     */
    @SuppressWarnings("unchecked")
    public QB queryName(String queryName) {
        this.queryName = queryName;
        return (QB) this;
    }
    
    /** Returns the boost for this query. */
    public float boost() {
        return this.boost;
    }
    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @SuppressWarnings("unchecked")
    @Override
    public QB boost(float boost) {
        this.boost = boost;
        return (QB) this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(parserName());
        Object valueToWrite = value;
        if (value instanceof BytesRef) {
            valueToWrite = ((BytesRef) value).utf8ToString();
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
    
    /** Returns a {@link QueryValidationException} if fieldName is null or empty, or if value is null. */
    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (fieldName == null || fieldName.isEmpty()) {
            validationException = QueryValidationException.addValidationError("field name cannot be null or empty.", validationException);
        }
        if (value == null) {
            validationException = QueryValidationException.addValidationError("value cannot be null.", validationException);
        }
        return validationException;        
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), fieldName, value, boost, queryName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        BaseTermQueryBuilder other = (BaseTermQueryBuilder) obj;
        return Objects.equals(fieldName, other.fieldName) &&
               Objects.equals(value, other.value) &&
               Objects.equals(boost, other.boost) &&
               Objects.equals(queryName, other.queryName);
    }

    /** Read the given parameters. */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        fieldName = in.readString();
        value = in.readGenericValue();
        boost = in.readFloat();
        queryName = in.readOptionalString();
    }

    /** Writes the given parameters. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(value);
        out.writeFloat(boost);
        out.writeOptionalString(queryName);
    }
}
