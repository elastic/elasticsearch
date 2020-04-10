/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An expression that evaluates to <code>true</code> if a field (map element) matches
 * the provided values. A <em>field</em> expression may have more than one provided value, in which
 * case the expression is true if <em>any</em> of the values are matched.
 */
public final class FieldExpression implements RoleMapperExpression {

    public static final String NAME = "field";

    private final String field;
    private final List<FieldValue> values;

    public FieldExpression(String field, List<FieldValue> values) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("null or empty field name (" + field + ")");
        }
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("null or empty values (" + values + ")");
        }
        this.field = field;
        this.values = values;
    }

    public FieldExpression(StreamInput in) throws IOException {
        this(in.readString(), in.readList(FieldValue::readFrom));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeList(values);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean match(ExpressionModel model) {
        return model.test(field, values);
    }

    public String getField() {
        return field;
    }

    public List<FieldValue> getValues() {
        return Collections.unmodifiableList(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final FieldExpression that = (FieldExpression) o;

        return this.field.equals(that.field) && this.values.equals(that.values);
    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + values.hashCode();
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(ExpressionParser.Fields.FIELD.getPreferredName());
        if (this.values.size() == 1) {
            builder.field(this.field);
            values.get(0).toXContent(builder, params);
        } else {
            builder.startArray(this.field);
            for (FieldValue fp : values) {
                fp.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder.endObject();
    }

    public static class FieldValue implements ToXContent, Writeable {
        private final Object value;
        @Nullable
        private final CharacterRunAutomaton automaton;

        public FieldValue(Object value) {
            this.value = value;
            this.automaton = buildAutomaton(value);
        }

        private static CharacterRunAutomaton buildAutomaton(Object value) {
            if (value instanceof String) {
                final String str = (String) value;
                if (Regex.isSimpleMatchPattern(str) || Automatons.isLuceneRegex(str)) {
                    return new CharacterRunAutomaton(Automatons.patterns(str));
                }
            }
            return null;
        }

        public Object getValue() {
            return value;
        }

        public CharacterRunAutomaton getAutomaton() {
            return automaton;
        }

        public static FieldValue readFrom(StreamInput in) throws IOException {
            return new FieldValue(in.readGenericValue());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGenericValue(value);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.value(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final FieldValue that = (FieldValue) o;
            return Objects.equals(this.value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

}
