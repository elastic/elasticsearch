/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

    DerivedMetricsPredicate MATCH_ALL = values -> true;

    /**
     * @param values the document's values indexed by slot, as filled by {@link DerivedMetricsSourceReader}
     */
    boolean test(Object[] values);

    /**
     * @param paths assigns a slot to each field the predicate reads, so evaluation is an array read rather than a path lookup
     */
    static DerivedMetricsPredicate compile(Map<String, Object> when, DerivedMetricsSourcePaths paths) {
        if (when == null) {
            return MATCH_ALL;
        }
        Map.Entry<String, Object> entry = single(when);
        String operator = entry.getKey();
        Object value = entry.getValue();
        return switch (operator) {
            case "exists" -> {
                int field = paths.slotFor((String) single(asMap(value, operator)).getValue());
                yield values -> hasValue(values[field]);
            }
            case "term" -> {
                Map.Entry<String, Object> term = single(asMap(value, operator));
                int field = paths.slotFor(term.getKey());
                Object expected = term.getValue();
                yield values -> matches(values[field], expected);
            }
            case "terms" -> {
                Map.Entry<String, Object> terms = single(asMap(value, operator));
                int field = paths.slotFor(terms.getKey());
                List<?> expected = (List<?>) terms.getValue();
                yield values -> {
                    Object actual = values[field];
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
                int field = paths.slotFor(range.getKey());
                Map<String, Object> bounds = asMap(range.getValue(), operator);
                Double gt = bound(bounds, "gt");
                Double gte = bound(bounds, "gte");
                Double lt = bound(bounds, "lt");
                Double lte = bound(bounds, "lte");
                yield values -> {
                    Object actual = values[field];
                    for (Object candidate : valuesOf(actual)) {
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
                List<DerivedMetricsPredicate> children = compileAll(value, operator, paths);
                yield values -> {
                    for (DerivedMetricsPredicate child : children) {
                        if (child.test(values) == false) {
                            return false;
                        }
                    }
                    return true;
                };
            }
            case "or" -> {
                List<DerivedMetricsPredicate> children = compileAll(value, operator, paths);
                yield values -> {
                    for (DerivedMetricsPredicate child : children) {
                        if (child.test(values)) {
                            return true;
                        }
                    }
                    return false;
                };
            }
            case "not" -> {
                DerivedMetricsPredicate child = compile(asMap(value, operator), paths);
                yield values -> child.test(values) == false;
            }
            default -> throw new IllegalArgumentException("unsupported derived metrics predicate operator [" + operator + "]");
        };
    }

    private static List<DerivedMetricsPredicate> compileAll(Object value, String operator, DerivedMetricsSourcePaths paths) {
        if (value instanceof List<?> children) {
            List<DerivedMetricsPredicate> compiled = new ArrayList<>(children.size());
            for (Object child : children) {
                compiled.add(compile(asMap(child, operator), paths));
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
        for (Object candidate : valuesOf(actual)) {
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

    private static Collection<?> valuesOf(Object value) {
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
