/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.common.Strings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.joining;

/**
 * Tracks which views lead to each index pattern.
 */
public final class IndexViewOrigins {

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
            chainsByPattern.computeIfAbsent(pattern, k -> new LinkedHashSet<>()).add(chain);
        }
    }

    /**
     * Error message suffix naming the views that lead to {@code indexExpression}, or an empty string if there are none.
     */
    public String describe(String indexExpression) {
        String[] patterns = Strings.splitStringByCommaToArray(indexExpression);
        List<String> descriptions = new ArrayList<>();
        for (String pattern : patterns) {
            Set<List<String>> chains = chainsByPattern.get(pattern);
            if (chains != null) {
                descriptions.add(patterns.length == 1 ? describeChains(chains) : "[" + pattern + "] " + describeChains(chains));
            }
        }
        if (descriptions.isEmpty()) {
            return "";
        }
        return patterns.length == 1 ? " " + descriptions.getFirst() : " (" + String.join(", ", descriptions) + ")";
    }

    private static String describeChains(Set<List<String>> chains) {
        return (chains.size() == 1 ? "referenced by view " : "referenced by views ") + chains.stream()
            .map(chain -> "[" + String.join(" -> ", chain) + "]")
            .collect(joining(", "));
    }
}
