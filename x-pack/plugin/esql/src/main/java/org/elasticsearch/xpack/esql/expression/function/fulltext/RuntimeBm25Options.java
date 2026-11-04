/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.Build;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.Expression.TypeResolution;

/**
 * The {@code similarity_function} option of {@code MATCH} and {@code MATCH_PHRASE}, which selects BM25 scoring for
 * text that has no index behind it. {@code boolean} keeps today's scoring and is the default.
 * <p>
 * The key is accepted only in snapshot builds. It is an ES|QL option: it is removed before the remaining options
 * reach the Query DSL builders, which reject unknown keys. {@code Knn} has its own {@code similarity_function}, so
 * nothing here lives in the shared base class.
 */
public final class RuntimeBm25Options {

    public static final String SIMILARITY_FUNCTION_OPTION = "similarity_function";

    /** Options that expand terms at search time, which the statistics pass cannot count. A 400 with {@code bm25}. */
    public static final List<String> REJECTED_WITH_BM25 = List.of(
        "fuzziness",
        "prefix_length",
        "max_expansions",
        "fuzzy_transpositions",
        "fuzzy_rewrite"
    );

    public enum RuntimeSimilarity {
        BOOLEAN,
        BM25;

        /** Case-insensitive; returns null for a value that names no similarity. */
        @Nullable
        static RuntimeSimilarity parse(String value) {
            for (RuntimeSimilarity similarity : values()) {
                if (similarity.name().equalsIgnoreCase(value)) {
                    return similarity;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private RuntimeBm25Options() {}

    /** {@code base} plus {@code similarity_function} in snapshot builds. */
    public static Map<String, DataType> allowedOptions(Map<String, DataType> base) {
        return allowedOptions(base, Build.current().isSnapshot());
    }

    static Map<String, DataType> allowedOptions(Map<String, DataType> base, boolean snapshot) {
        if (snapshot == false) {
            return base;
        }
        Map<String, DataType> allowed = new LinkedHashMap<>(base);
        allowed.put(SIMILARITY_FUNCTION_OPTION, DataType.KEYWORD);
        return Map.copyOf(allowed);
    }

    /** Whether the options set {@code similarity_function}, whatever its value. */
    public static boolean isSet(@Nullable Expression options) {
        return options instanceof MapExpression map && map.keyFoldedMap().containsKey(SIMILARITY_FUNCTION_OPTION);
    }

    /** The selected similarity, {@code boolean} when the key is absent or its value is invalid. */
    public static RuntimeSimilarity similarity(@Nullable Expression options) {
        String value = value(options);
        RuntimeSimilarity similarity = value == null ? null : RuntimeSimilarity.parse(value);
        return similarity == null ? RuntimeSimilarity.BOOLEAN : similarity;
    }

    /** Whether the user gave a key other than {@code similarity_function}, which a Query DSL builder must apply. */
    public static boolean hasQueryBuilderOptions(@Nullable Expression options) {
        return options instanceof MapExpression map
            && map.keyFoldedMap().keySet().stream().anyMatch(key -> key.equals(SIMILARITY_FUNCTION_OPTION) == false);
    }

    /** The given option map without {@code similarity_function}. */
    public static Map<String, Object> queryBuilderOptions(Map<String, Object> options) {
        if (options.containsKey(SIMILARITY_FUNCTION_OPTION) == false) {
            return options;
        }
        Map<String, Object> copy = new HashMap<>(options);
        copy.remove(SIMILARITY_FUNCTION_OPTION);
        return copy;
    }

    /** Rejects a value other than {@code bm25} and {@code boolean}, in the style of the other option errors. */
    public static TypeResolution resolveValue(@Nullable Expression options, String functionSource) {
        if (options instanceof MapExpression map && map.keyFoldedMap().get(SIMILARITY_FUNCTION_OPTION) instanceof Literal literal) {
            String value = BytesRefs.toString(literal.value());
            if (value == null || RuntimeSimilarity.parse(value) == null) {
                return new TypeResolution(
                    "Invalid option [" + SIMILARITY_FUNCTION_OPTION + "] in [" + functionSource + "], expected one of [bm25, boolean]"
                );
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Nullable
    private static String value(@Nullable Expression options) {
        if (options instanceof MapExpression map && map.keyFoldedMap().get(SIMILARITY_FUNCTION_OPTION) instanceof Literal literal) {
            return BytesRefs.toString(literal.value());
        }
        return null;
    }
}
