/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.util.Map;
import java.util.Set;

/**
 * Fails closed for mappings the prototype's raw-source loader cannot reproduce. This is deliberately
 * conservative: every visible nested field is checked, including fields not used by this query.
 * The caller must supply authorized field caps, never the unrestricted mapping's field names.
 */
public final class NestedReductionsSourceValidation {
    private static final Set<String> TRANSFORMATIONS = Set.of(
        "null_value",
        "normalizer",
        "ignore_above",
        "ignore_malformed",
        "coerce",
        "script",
        "copy_to",
        "subobjects"
    );

    private NestedReductionsSourceValidation() {}

    /** Reject unavailable or reshaped source before execution, without exposing unauthorized mapping fields. */
    public static void validate(String index, Settings settings, Map<String, ?> mapping, Set<String> visible) {
        if (IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.get(settings) != SourceFieldMapper.Mode.STORED) {
            throw unsupported(index, "requires stored, unfiltered _source");
        }
        if (mapping.get("_source") instanceof Map<?, ?> source) {
            if ((source.containsKey("enabled") && Booleans.parseBoolean(source.get("enabled").toString()) == false)
                || (source.containsKey("mode") && "stored".equals(source.get("mode")) == false)
                || nonempty(source.get("includes"))
                || nonempty(source.get("excludes"))) {
                throw unsupported(index, "requires stored, unfiltered _source");
            }
        }
        if (settings.getAsBoolean("index.mapping.ignore_malformed", false)
            || settings.getAsBoolean("index.mapping.coerce", true) == false
            || IndexSettings.IGNORE_ABOVE_SETTING.get(settings) != Integer.MAX_VALUE) {
            throw unsupported(index, "does not support index-level value transformations");
        }
        walk(index, "", mapping, visible, false, false);
    }

    /**
     * Mapping keys that are present but equal the value the mapper uses when the key is omitted.
     * {@code coerce: true} and {@code ignore_malformed: false} do not rewrite stored source.
     */
    private static boolean explicitDefault(String parameter, Object value) {
        return switch (parameter) {
            case "coerce" -> Boolean.TRUE.equals(value) || "true".equals(value);
            case "ignore_malformed" -> Boolean.FALSE.equals(value) || "false".equals(value);
            default -> false;
        };
    }

    private static boolean nonempty(Object value) {
        return value != null
            && (value instanceof java.util.Collection<?> values ? values.isEmpty() == false : value.toString().isEmpty() == false);
    }

    private static void walk(String index, String path, Map<?, ?> field, Set<String> visible, boolean nested, boolean multifield) {
        // A hidden source can copy into a visible destination. Inspect the destination without revealing the source's name.
        Object copies = field.get("copy_to");
        if (copies != null
            && (copies instanceof java.util.Collection<?> names
                ? names.stream().anyMatch(visible::contains)
                : visible.contains(copies.toString()))) {
            throw unsupported(index, "does not support copy_to into visible fields");
        }
        String prefix = path + ".";
        boolean isVisible = path.isEmpty() || visible.stream().anyMatch(name -> name.equals(path) || name.startsWith(prefix));
        nested |= "nested".equals(field.get("type"));
        if (nested && visible.contains(path)) {
            if (multifield || "alias".equals(field.get("type"))) {
                throw unsupported(index, "does not support aliases or multifields [" + path + "]");
            }
            // Float encodings round source values. Widening their ES|QL type to double does not undo that rounding.
            if (field.get("type") != null && Set.of("float", "half_float", "scaled_float").contains(field.get("type"))) {
                throw unsupported(index, "does not support rounded numeric mappings [" + path + "]");
            }
            for (String parameter : TRANSFORMATIONS) {
                // An explicit key whose value equals the mapper default does not change stored source.
                if (field.containsKey(parameter) && explicitDefault(parameter, field.get(parameter)) == false) {
                    throw unsupported(index, "does not support mapping parameter [" + parameter + "] on [" + path + "]");
                }
            }
        }
        if (field.containsKey("subobjects") && isVisible) {
            throw unsupported(index, "does not support explicit subobjects settings");
        }
        for (String key : Set.of("properties", "fields")) {
            if (field.get(key) instanceof Map<?, ?> children) {
                for (var entry : children.entrySet()) {
                    if (entry.getValue() instanceof Map<?, ?> child) {
                        String name = path.isEmpty() ? entry.getKey().toString() : path + "." + entry.getKey();
                        walk(index, name, child, visible, nested, multifield || key.equals("fields"));
                    }
                }
            }
        }
    }

    private static IllegalArgumentException unsupported(String index, String reason) {
        return new IllegalArgumentException("NESTED source-backed PoC " + reason + " in index [" + index + "]");
    }
}
