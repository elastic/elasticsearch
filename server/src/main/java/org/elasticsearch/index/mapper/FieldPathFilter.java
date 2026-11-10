/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.regex.Regex;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Filters the parsing of a mapping to a limited set of fields
 * <p>
 * All field parents are included: both container fields such as objects,
 * and multi-field parents.
 * <p>
 * Filters for patterns track the position of the parser.  A new one must be made
 * for each parse, and should not be shared across threads. The filter that matches
 * all fields is stateless and can be shared.
 */
public abstract class FieldPathFilter {

    private static final FieldPathFilter MATCH_ALL = new MatchAll();

    private FieldPathFilter() {}

    /**
     * A filter that keeps the fields matched by {@code fieldPatterns}, each of which is a field path that may use {@code *}
     * as a wildcard, as in {@link org.elasticsearch.common.regex.Regex#simpleMatch}.
     */
    public static FieldPathFilter forPatterns(Collection<String> fieldPatterns) {
        for (String pattern : fieldPatterns) {
            if (pattern.chars().allMatch(c -> c == '*')) {
                return MATCH_ALL;
            }
        }
        return new PatternFilter(List.copyOf(fieldPatterns));
    }

    /**
     * A filter that keeps every field. It does not track the position of the parser, so it can be shared.
     */
    public static FieldPathFilter matchAll() {
        return MATCH_ALL;
    }

    /**
     * Enters the property {@code name} of the object being parsed, which may be a dotted path, with the {@code value} it has in
     * the mapping. A property is skipped if it cannot lead to a matching field: its path is not a prefix of a match, or it is a
     * field with nothing below it that does not match, whereas an object or a field with multi-fields that match is kept.
     * The value is not necessarily a map, in which case only the name is considered.
     *
     * @return {@code true} if the property is to be parsed, in which case the caller has to {@link #exit} afterwards.
     *         If {@code false} the property is to be skipped and there is nothing to exit.
     */
    abstract boolean enter(String name, Object value);

    abstract void exit();

    /**
     * Copies the decoded root object of a mapping so that it can be parsed destructively.
     * The properties that this filter does not match are left out of the copy, as the parse would skip them anyway.
     * This is done for the properties of the root and of the object and nested fields below it. Any other field is copied
     * whole, as only objects are known to have properties that are fields: a flattened field has properties of its own kind,
     * and the {@code properties} of a field's metadata are not properties at all.
     */
    abstract Map<String, Object> filteredCopy(Map<String, Object> root);

    private static final class PatternFilter extends FieldPathFilter {

        private final List<String> patterns;
        private final Deque<String> path = new ArrayDeque<>();

        private PatternFilter(List<String> patterns) {
            this.patterns = patterns;
        }

        @Override
        boolean enter(String name, Object value) {
            String fullPath = path.isEmpty() ? name : path.peek() + "." + name;
            if (mayMatchFieldAtOrBelow(fullPath) == false || (value instanceof Map<?, ?> node && canBeSkipped(fullPath, node))) {
                return false;
            }
            path.push(fullPath);
            return true;
        }

        @Override
        void exit() {
            path.pop();
        }

        private boolean canBeSkipped(String fullPath, Map<?, ?> node) {
            if (matchesAnyPattern(fullPath) || node.get("properties") instanceof Map) {
                return false;
            }
            if (node.get("fields") instanceof Map<?, ?> multiFields) {
                for (Object multiFieldName : multiFields.keySet()) {
                    if (matchesAnyPattern(fullPath + "." + multiFieldName)) {
                        return false;
                    }
                }
            }
            return true;
        }

        private boolean matchesAnyPattern(String fieldPath) {
            for (String pattern : patterns) {
                if (Regex.simpleMatch(pattern, fieldPath)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        @SuppressWarnings("unchecked")
        Map<String, Object> filteredCopy(Map<String, Object> root) {
            Map<String, Object> copy = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : root.entrySet()) {
                if (entry.getKey().equals("properties") && entry.getValue() instanceof Map) {
                    Map<String, Object> kept = new LinkedHashMap<>();
                    for (Map.Entry<String, Object> property : ((Map<String, Object>) entry.getValue()).entrySet()) {
                        if (enter(property.getKey(), property.getValue())) {
                            Object value = property.getValue();
                            kept.put(
                                property.getKey(),
                                isObjectOrNested(value) ? filteredCopy((Map<String, Object>) value) : deepCopy(value)
                            );
                            exit();
                        }
                    }
                    copy.put(entry.getKey(), kept);
                } else {
                    copy.put(entry.getKey(), deepCopy(entry.getValue()));
                }
            }
            return copy;
        }

        // an object has a type of object or nested, or has no type but properties, which is how the parser tells them
        private static boolean isObjectOrNested(Object value) {
            if (value instanceof Map<?, ?> node) {
                Object type = node.get("type");
                if (type == null) {
                    return node.get("properties") instanceof Map;
                }
                return ObjectMapper.CONTENT_TYPE.equals(type) || NestedObjectMapper.CONTENT_TYPE.equals(type);
            }
            return false;
        }

        private boolean mayMatchFieldAtOrBelow(String fullPath) {
            for (String pattern : patterns) {
                if (mayMatchFieldAtOrBelow(pattern, fullPath)) {
                    return true;
                }
            }
            return false;
        }

        private static boolean mayMatchFieldAtOrBelow(String pattern, String fullPath) {
            int wildcard = pattern.indexOf('*');
            if (wildcard < 0) {
                return pattern.equals(fullPath) || pattern.startsWith(fullPath + ".");
            }
            // a wildcard may match dots too, so anything that starts like the pattern may match
            String literalPrefix = pattern.substring(0, wildcard);
            return fullPath.startsWith(literalPrefix) || literalPrefix.startsWith(fullPath + ".");
        }
    }

    private static final class MatchAll extends FieldPathFilter {

        @Override
        boolean enter(String name, Object value) {
            return true;
        }

        @Override
        void exit() {}

        @Override
        Map<String, Object> filteredCopy(Map<String, Object> root) {
            return deepCopy(root);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T deepCopy(T value) {
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> copy = new LinkedHashMap<>();
            map.forEach((k, v) -> copy.put(k, deepCopy(v)));
            return (T) copy;
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            list.forEach(v -> copy.add(deepCopy(v)));
            return (T) copy;
        }
        return value;
    }
}
