/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@code when} clause of a derived metric, compiled once into a tree that can be evaluated against a document's source on the write
 * path.
 *
 * <p>The supported operators are deliberately limited to the script-free forms validated by
 * {@link org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics}. Compilation assumes that validation has already happened, so it
 * fails with {@link IllegalArgumentException} only on shapes that validation would have rejected.
 *
 * <p>Evaluation is intentionally lenient about types, because a source document is not guaranteed to agree with the mapping: a value
 * matches a {@code term} if it is equal, numerically equal, or equal once both sides are rendered as strings. Multi-valued fields match
 * if any of their values match.
 */
@FunctionalInterface
public interface DerivedMetricsPredicate {

    DerivedMetricsPredicate MATCH_ALL = source -> true;

    boolean test(Map<String, Object> source);

    static DerivedMetricsPredicate compile(Map<String, Object> when) {
        if (when == null) {
            return MATCH_ALL;
        }
        Map.Entry<String, Object> entry = single(when);
        String operator = entry.getKey();
        Object value = entry.getValue();
        return switch (operator) {
            case "exists" -> {
                String[] field = path((String) single(asMap(value, operator)).getValue());
                yield source -> hasValue(XContentMapValues.extractValue(source, field));
            }
            case "term" -> {
                Map.Entry<String, Object> term = single(asMap(value, operator));
                String[] field = path(term.getKey());
                Object expected = term.getValue();
                yield source -> matches(XContentMapValues.extractValue(source, field), expected);
            }
            case "terms" -> {
                Map.Entry<String, Object> terms = single(asMap(value, operator));
                String[] field = path(terms.getKey());
                List<?> expected = (List<?>) terms.getValue();
                yield source -> {
                    Object actual = XContentMapValues.extractValue(source, field);
                    for (Object candidate : expected) {
                        if (matches(actual, candidate)) {
                            return true;
                        }
                    }
                    return false;
                };
            }
            case "range" -> {
                Map.Entry<String, Object> range = single(asMap(value, operator));
                String[] field = path(range.getKey());
                Map<String, Object> bounds = asMap(range.getValue(), operator);
                Double gt = bound(bounds, "gt");
                Double gte = bound(bounds, "gte");
                Double lt = bound(bounds, "lt");
                Double lte = bound(bounds, "lte");
                yield source -> {
                    Object actual = XContentMapValues.extractValue(source, field);
                    for (Object candidate : values(actual)) {
                        Double number = asDouble(candidate);
                        if (number == null) {
                            continue;
                        }
                        if (gt != null && number <= gt) {
                            continue;
                        }
                        if (gte != null && number < gte) {
                            continue;
                        }
                        if (lt != null && number >= lt) {
                            continue;
                        }
                        if (lte != null && number > lte) {
                            continue;
                        }
                        return true;
                    }
                    return false;
                };
            }
            case "and" -> {
                List<DerivedMetricsPredicate> children = compileAll(value, operator);
                yield source -> {
                    for (DerivedMetricsPredicate child : children) {
                        if (child.test(source) == false) {
                            return false;
                        }
                    }
                    return true;
                };
            }
            case "or" -> {
                List<DerivedMetricsPredicate> children = compileAll(value, operator);
                yield source -> {
                    for (DerivedMetricsPredicate child : children) {
                        if (child.test(source)) {
                            return true;
                        }
                    }
                    return false;
                };
            }
            case "not" -> {
                DerivedMetricsPredicate child = compile(asMap(value, operator));
                yield source -> child.test(source) == false;
            }
            default -> throw new IllegalArgumentException("unsupported derived metrics predicate operator [" + operator + "]");
        };
    }

    /**
     * Splits a dotted path once, at compile time. {@link XContentMapValues#extractValue(String, Map)} splits on every call, which on the
     * write path means a regular expression split per predicate per document.
     */
    private static String[] path(String field) {
        return field.split("\\.");
    }

    /**
     * Collects every source path the predicate reads, so that only those paths need to be extracted from {@code _source}.
     */
    static void collectPaths(Map<String, Object> when, Set<String> paths) {
        if (when == null) {
            return;
        }
        Map.Entry<String, Object> entry = single(when);
        String operator = entry.getKey();
        Object value = entry.getValue();
        switch (operator) {
            case "exists" -> paths.add((String) single(asMap(value, operator)).getValue());
            case "term", "terms", "range" -> paths.add(single(asMap(value, operator)).getKey());
            case "and", "or" -> {
                for (Object child : (List<?>) value) {
                    collectPaths(asMap(child, operator), paths);
                }
            }
            case "not" -> collectPaths(asMap(value, operator), paths);
            default -> throw new IllegalArgumentException("unsupported derived metrics predicate operator [" + operator + "]");
        }
    }

    private static List<DerivedMetricsPredicate> compileAll(Object value, String operator) {
        if (value instanceof List<?> children) {
            List<DerivedMetricsPredicate> compiled = new ArrayList<>(children.size());
            for (Object child : children) {
                compiled.add(compile(asMap(child, operator)));
            }
            return List.copyOf(compiled);
        }
        throw new IllegalArgumentException("derived metrics predicate [" + operator + "] must be an array");
    }

    private static boolean hasValue(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Collection<?> collection) {
            return collection.stream().anyMatch(DerivedMetricsPredicate::hasValue);
        }
        return true;
    }

    private static boolean matches(Object actual, Object expected) {
        for (Object candidate : values(actual)) {
            if (candidate == null) {
                continue;
            }
            if (candidate.equals(expected)) {
                return true;
            }
            Double candidateNumber = asDouble(candidate);
            Double expectedNumber = asDouble(expected);
            if (candidateNumber != null && expectedNumber != null) {
                if (candidateNumber.doubleValue() == expectedNumber.doubleValue()) {
                    return true;
                }
                continue;
            }
            if (String.valueOf(candidate).equals(String.valueOf(expected))) {
                return true;
            }
        }
        return false;
    }

    private static Collection<?> values(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof Collection<?> collection) {
            return collection;
        }
        return List.of(value);
    }

    private static Double asDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String string) {
            try {
                return Double.valueOf(string);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static Double bound(Map<String, Object> bounds, String name) {
        Object value = bounds.get(name);
        return value == null ? null : ((Number) value).doubleValue();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value, String operator) {
        if (value instanceof Map<?, ?> map) {
            // the config model validates that predicate keys are strings before this is ever reached
            return (Map<String, Object>) map;
        }
        throw new IllegalArgumentException("derived metrics predicate [" + operator + "] must be an object");
    }

    private static Map.Entry<String, Object> single(Map<String, Object> map) {
        if (map.size() != 1) {
            throw new IllegalArgumentException("derived metrics predicate must contain exactly one entry but was " + map.keySet());
        }
        return map.entrySet().iterator().next();
    }
}
