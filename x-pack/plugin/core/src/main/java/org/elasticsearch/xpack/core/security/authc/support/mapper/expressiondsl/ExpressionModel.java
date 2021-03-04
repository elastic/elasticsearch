/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;

import java.util.Collection;
import java.util.Collections;
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

    private static final Logger logger = LogManager.getLogger();

    private final Map<String, Object> fieldValues;
    private final Map<String, Predicate<FieldExpression.FieldValue>> fieldPredicates;

    public ExpressionModel() {
        this.fieldValues = new HashMap<>();
        this.fieldPredicates = new HashMap<>();
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
        this.fieldValues.put(name, value);
        this.fieldPredicates.put(name, predicate);
        return this;
    }

    /**
     * Returns {@code true} if the named field, matches <em>any</em> of the provided values.
     */
    public boolean test(String field, List<FieldExpression.FieldValue> values) {
        final Predicate<FieldExpression.FieldValue> predicate = this.fieldPredicates.getOrDefault(field, NULL_PREDICATE);
        boolean isMatch = values.stream().anyMatch(predicate);
        if (isMatch == false && predicate == NULL_PREDICATE && fieldPredicates.containsKey(field) == false) {
            logger.debug(() -> new ParameterizedMessage("Attempt to test field [{}] against value(s) [{}]," +
                " but the field [{}] does not have a value on this object;" +
                " known fields are [{}]",
                field, Strings.collectionToCommaDelimitedString(values),
                field, Strings.collectionToCommaDelimitedString(fieldPredicates.keySet())));
        }

        return isMatch;
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

    public Map<String, Object> asMap() {
        return Collections.unmodifiableMap(fieldValues);
    }

    @Override
    public String toString() {
        return fieldValues.toString();
    }
}
