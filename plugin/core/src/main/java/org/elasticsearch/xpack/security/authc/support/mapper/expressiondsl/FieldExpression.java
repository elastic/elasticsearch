/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl;

import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.security.support.Automatons;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * An expression that evaluates to <code>true</code> if a field (map element) matches
 * the provided values. A <em>field</em> expression may have more than one provided value, in which
 * case the expression is true if <em>any</em> of the values are matched.
 */
public final class FieldExpression implements RoleMapperExpression {

    static final String NAME = "field";

    private final String field;
    private final List<FieldPredicate> values;

    public FieldExpression(String field, List<FieldPredicate> values) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("null or empty field name (" + field + ")");
        }
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("null or empty values (" + values + ")");
        }
        this.field = field;
        this.values = values;
    }

    FieldExpression(StreamInput in) throws IOException {
        this(in.readString(), in.readList(FieldPredicate::readFrom));
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
    public boolean match(Map<String, Object> object) {
        final Object fieldValue = object.get(field);
        if (fieldValue instanceof Collection) {
            return ((Collection) fieldValue).stream().anyMatch(this::matchValue);
        } else {
            return matchValue(fieldValue);
        }
    }

    private boolean matchValue(Object fieldValue) {
        return values.stream().anyMatch(predicate -> predicate.test(fieldValue));
    }

    public String getField() {
        return field;
    }

    public List<Predicate<Object>> getValues() {
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
            for (FieldPredicate fp : values) {
                fp.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder.endObject();
    }

    /**
     * A special predicate for matching values in a {@link FieldExpression}. This interface
     * exists to support the serialisation ({@link ToXContent}, {@link Writeable}) of <em>field</em>
     * expressions.
     */
    public static class FieldPredicate implements Predicate<Object>, ToXContent, Writeable {
        private final Object value;
        private final Predicate<Object> predicate;

        private FieldPredicate(Object value, Predicate<Object> predicate) {
            this.value = value;
            this.predicate = predicate;
        }

        @Override
        public boolean test(Object o) {
            return this.predicate.test(o);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params)
                throws IOException {
            return builder.value(value);
        }

        /**
         * Create an appropriate predicate based on the type and value of the argument.
         * The predicate is formed according to the following rules:
         * <ul>
         * <li>If <code>value</code> is <code>null</code>, then the predicate evaluates to
         * <code>true</code> <em>if-and-only-if</em> the predicate-argument is
         * <code>null</code></li>
         * <li>If <code>value</code> is a {@link Boolean}, then the predicate
         * evaluates to <code>true</code> <em>if-and-only-if</em> the predicate-argument is
         * an {@link Boolean#equals(Object) equal} <code>Boolean</code></li>
         * <li>If <code>value</code> is a {@link Number}, then the predicate
         * evaluates to <code>true</code> <em>if-and-only-if</em> the predicate-argument is
         * numerically equal to <code>value</code>. This class makes a best-effort to determine
         * numeric equality across different implementations of <code>Number</code>, but the
         * implementation can only be guaranteed for standard integral representations (
         * <code>Long</code>, <code>Integer</code>, etc)</li>
         * <li>If <code>value</code> is a {@link String}, then it is treated as a
         * {@link org.apache.lucene.util.automaton.Automaton Lucene automaton} pattern with
         * {@link Automatons#predicate(String...) corresponding predicate}.
         * </li>
         * </ul>
         */
        public static FieldPredicate create(Object value) {
            Predicate<Object> predicate = buildPredicate(value);
            return new FieldPredicate(value, predicate);
        }

        public static FieldPredicate readFrom(StreamInput in) throws IOException {
            return create(in.readGenericValue());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGenericValue(value);
        }

        private static Predicate<Object> buildPredicate(Object object) {
            if (object == null) {
                return Objects::isNull;
            }
            if (object instanceof Boolean) {
                return object::equals;
            }
            if (object instanceof Number) {
                return (other) -> numberEquals((Number) object, other);
            }
            if (object instanceof String) {
                final String str = (String) object;
                if (str.isEmpty()) {
                    return obj -> String.valueOf(obj).isEmpty();
                } else {
                    final Predicate<String> predicate = Automatons.predicate(str);
                    return obj -> predicate.test(String.valueOf(obj));
                }
            }
            throw new IllegalArgumentException("Unsupported value type " + object.getClass());
        }

        private static boolean numberEquals(Number left, Object other) {
            if (left.equals(other)) {
                return true;
            }
            if ((other instanceof Number) == false) {
                return false;
            }
            Number right = (Number) other;
            if (left instanceof Double || left instanceof Float
                    || right instanceof Double || right instanceof Float) {
                return Double.compare(left.doubleValue(), right.doubleValue()) == 0;
            }
            return Numbers.toLongExact(left) == Numbers.toLongExact(right);
        }

    }

}
