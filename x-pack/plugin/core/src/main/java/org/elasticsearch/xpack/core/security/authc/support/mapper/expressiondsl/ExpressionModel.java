/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl;

import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.collect.Tuple;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Represents the "model" object to be evaluated within a {@link RoleMapperExpression}.
 * The model is a flat object, where fields are defined by strings and value is either a
 * string, boolean, or number, or a collection of the above.
 */
public class ExpressionModel {

    public static final Predicate<FieldExpression.FieldValue> NULL_PREDICATE = field -> field.getValue() == null;
    private Map<String, Tuple<Object, Predicate<FieldExpression.FieldValue>>> fields;

    public ExpressionModel() {
        this.fields = new HashMap<>();
    }

    /**
     * Defines a field using a predicate that corresponds to the type of {@code value}
     *
     * @see #buildPredicate(Object)
     */
    public ExpressionModel defineField(String name, Object value) {
        return defineField(name, value, buildPredicate(value));
    }

    /**
     * Defines a field using a supplied predicate.
     */
    public ExpressionModel defineField(String name, Object value, Predicate<FieldExpression.FieldValue> predicate) {
        this.fields.put(name, new Tuple<>(value, predicate));
        return this;
    }

    /**
     * Returns {@code true} if the named field, matches <em>any</em> of the provided values.
     */
    public boolean test(String field, List<FieldExpression.FieldValue> values) {
        final Tuple<Object, Predicate<FieldExpression.FieldValue>> tuple = this.fields.get(field);
        final Predicate<FieldExpression.FieldValue> predicate;
        if (tuple == null) {
            predicate = NULL_PREDICATE;
        } else {
            predicate = tuple.v2();
        }
        return values.stream().anyMatch(predicate);
    }

    /**
     * Constructs a {@link Predicate} that matches correctly based on the type of the provided parameter.
     */
    static Predicate<FieldExpression.FieldValue> buildPredicate(Object object) {
        if (object == null) {
            return NULL_PREDICATE;
        }
        if (object instanceof Boolean) {
            return field -> object.equals(field.getValue());
        }
        if (object instanceof Number) {
            return field -> numberEquals((Number) object, field.getValue());
        }
        if (object instanceof String) {
            return field -> field.getAutomaton() == null ? object.equals(field.getValue()) : field.getAutomaton().run((String) object);
        }
        if (object instanceof Collection) {
            return ((Collection<?>) object).stream()
                    .map(element -> buildPredicate(element))
                    .reduce((a, b) -> a.or(b))
                    .orElse(fieldValue -> false);
        }
        throw new IllegalArgumentException("Unsupported value type " + object.getClass());
    }

    /**
     * A comparison of {@link Number} objects that compares by floating point when either value is a {@link Float} or {@link Double}
     * otherwise compares by {@link Numbers#toLongExact long}.
     */
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
