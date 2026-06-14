/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.lang.foreign.SymbolLookup;

/**
 * Resolves the actual native symbol name to bind for a given base name.
 * May probe variant names (e.g. capability-level suffixes).
 * Must throw {@link LinkageError} if no suitable symbol is found.
 * Implementing classes must have a public no-arg constructor.
 */
@FunctionalInterface
public interface SymbolResolver {
    /**
     * Given a base symbol name and lookup, returns the actual symbol name to bind.
     *
     * @param baseName the base C symbol name from {@code @Function}
     * @param lookup the symbol lookup to probe
     * @return the resolved symbol name
     * @throws LinkageError if no suitable symbol is found
     */
    String resolve(String baseName, SymbolLookup lookup);
}
