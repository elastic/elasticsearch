/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.common.Strings;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableMap;

/**
 * Tracks which views lead to each index pattern.
 *
 * @param chainsByPattern index pattern read by a view, mapped to the view chains leading to it
 */
public record IndexViewOrigins(Map<String, Set<List<String>>> chainsByPattern) {

    public static final IndexViewOrigins EMPTY = new IndexViewOrigins(Map.of());

    public IndexViewOrigins {
        chainsByPattern = chainsByPattern.entrySet().stream().collect(toUnmodifiableMap(Map.Entry::getKey, e -> Set.copyOf(e.getValue())));
    }

    /**
     * Error message suffix naming the views that lead to {@code indexExpression}, if there are any.
     */
    public Optional<String> describe(String indexExpression) {
        Set<String> indices = new LinkedHashSet<>();
        for (String pattern : Strings.splitStringByCommaToArray(indexExpression)) {
            if (isExclusion(pattern) == false) {
                indices.add(pattern);
            }
        }
        Map<String, String> descriptions = new LinkedHashMap<>();
        for (String index : indices) {
            Set<List<String>> chains = chainsByPattern.get(index);
            if (chains != null) {
                descriptions.put(index, describeChains(chains));
            }
        }
        if (descriptions.isEmpty()) {
            return Optional.empty();
        }
        Set<String> distinct = new HashSet<>(descriptions.values());
        if (descriptions.size() == indices.size() && distinct.size() == 1) {
            return Optional.of(distinct.iterator().next());
        }
        return Optional.of(
            descriptions.entrySet().stream().map(e -> "[" + e.getKey() + "] " + e.getValue()).collect(joining("; ", "(", ")"))
        );
    }

    private static boolean isExclusion(String pattern) {
        return pattern.startsWith("-") || pattern.contains(":-");
    }

    /**
     * Whether {@code pattern} appears in the body of {@code viewName} itself, rather than only in views nested inside it. A
     * pattern recorded under several chains counts if any of them ends at {@code viewName}.
     */
    public boolean readDirectlyBy(String pattern, String viewName) {
        Set<List<String>> chains = chainsByPattern.get(pattern);
        if (chains == null) {
            return false;
        }
        for (List<String> chain : chains) {
            if (chain.getLast().equals(viewName)) {
                return true;
            }
        }
        return false;
    }

    private static String describeChains(Set<List<String>> chains) {
        List<String> rendered = chains.stream().map(chain -> "[" + String.join(" -> ", chain) + "]").sorted().toList();
        return (rendered.size() == 1 ? "referenced by view " : "referenced by views ") + String.join(", ", rendered);
    }

    /**
     * Collects view chains while views are resolved.
     */
    static final class Builder {

        private final Map<String, Set<List<String>>> chainsByPattern = new HashMap<>();

        /**
         * Records that {@code indexPattern} is read through {@code viewChain}, outermost view first.
         */
        void record(String indexPattern, Collection<String> viewChain) {
            if (viewChain.isEmpty()) {
                return;
            }
            List<String> chain = List.copyOf(viewChain);
            for (String pattern : Strings.splitStringByCommaToArray(indexPattern)) {
                chainsByPattern.computeIfAbsent(pattern, k -> new HashSet<>()).add(chain);
            }
        }

        IndexViewOrigins build() {
            return new IndexViewOrigins(chainsByPattern);
        }
    }
}
