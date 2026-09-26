/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.analysis.common;

import org.elasticsearch.index.analysis.TokenFilterFactory;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * A wrapper around the {@code allFilters} lookup passed to
 * {@link TokenFilterFactory#getChainAwareTokenFilterFactory} that detects reference cycles between
 * token filters which recursively rewrite other filters they refer to by name (for example
 * {@code multiplexer} and {@code condition}).
 *
 * <p>Such a filter resolves its referenced filters and calls
 * {@link TokenFilterFactory#getChainAwareTokenFilterFactory} on each of them. If a filter refers to
 * itself, directly or through a chain of other referring filters, this rewriting recurses forever
 * and blows the stack.
 */
final class ReferringFilterResolver implements Function<String, TokenFilterFactory> {
    private final Function<String, TokenFilterFactory> delegate;
    private final Set<String> expansionPath;

    private ReferringFilterResolver(Function<String, TokenFilterFactory> delegate, Set<String> expansionPath) {
        this.delegate = delegate;
        this.expansionPath = expansionPath;
    }

    @Override
    public TokenFilterFactory apply(String name) {
        return delegate.apply(name);
    }

    /**
     * Records that the referring filter {@code filterName} is about to expand the filters it refers
     * to, returning a resolver to use for that expansion. Throws {@link IllegalArgumentException} if
     * {@code filterName} is already being expanded higher up the chain, which indicates a reference
     * cycle.
     *
     * @param filterName the name of the referring token filter (e.g. a {@code multiplexer} or
     *                   {@code condition} filter) that is about to rewrite its referenced filters
     * @param allFilters the {@code allFilters} lookup received by the referring filter, which may
     *                   itself be a {@link ReferringFilterResolver} created by an ancestor
     */
    static ReferringFilterResolver enter(String filterName, Function<String, TokenFilterFactory> allFilters) {
        if (allFilters instanceof ReferringFilterResolver resolver) {
            if (resolver.expansionPath.contains(filterName)) {
                throw new IllegalArgumentException(
                    "Token filter ["
                        + filterName
                        + "] refers to itself, either directly or via a cycle of filters "
                        + resolver.expansionPath
                );
            }
            Set<String> nextPath = new LinkedHashSet<>(resolver.expansionPath);
            nextPath.add(filterName);
            return new ReferringFilterResolver(resolver.delegate, nextPath);
        }
        Set<String> path = new LinkedHashSet<>();
        path.add(filterName);
        return new ReferringFilterResolver(allFilters, path);
    }
}
