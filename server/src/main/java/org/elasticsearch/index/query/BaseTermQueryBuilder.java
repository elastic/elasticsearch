/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public abstract class BaseTermQueryBuilder<QB extends BaseTermQueryBuilder<QB>> extends AbstractQueryBuilder<QB> {

    public static final ParseField VALUE_FIELD = new ParseField("value");

    /** Name of field to match against. */
    protected final String fieldName;

    /** Value to find matches for. */
    protected final Object value;

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
     * In case value is assigned to a string, we internally convert it to a {@link BytesRef}
     * because in {@link TermQueryBuilder} and {@link SpanTermQueryBuilder} string values are parsed to {@link BytesRef}
     * and we want internal representation of query to be equal regardless of whether it was created from XContent or via Java API.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, Object value) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
        this.fieldName = fieldName;
        this.value = maybeConvertToBytesRef(value);
    }

    /**
     * Read from a stream.
     */
    protected BaseTermQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        value = in.readGenericValue();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(value);
    }

    /** Returns the field name used in this query. */
    public String fieldName() {
        return this.fieldName;
    }

    /**
     *  Returns the value used in this query.
     *  If necessary, converts internal {@link BytesRef} representation back to string.
     */
    public Object value() {
        return maybeConvertToString(this.value);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.startObject(fieldName);
        builder.field(VALUE_FIELD.getPreferredName(), maybeConvertToString(this.value));
        addExtraXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    protected void addExtraXContent(XContentBuilder builder, Params params) throws IOException {
        // Do nothing but allows subclasses to override.
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value);
    }

    @Override
    protected boolean doEquals(QB other) {
        return Objects.equals(fieldName, other.fieldName) && Objects.equals(value, other.value);
    }
}
