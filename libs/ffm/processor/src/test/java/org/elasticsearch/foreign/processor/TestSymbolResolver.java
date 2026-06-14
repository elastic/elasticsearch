/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.SymbolResolver;

import java.lang.foreign.SymbolLookup;

/**
 * A no-op {@link SymbolResolver} used in annotation processor tests.
 * Returns the base name unchanged.
 */
public class TestSymbolResolver implements SymbolResolver {
    @Override
    public String resolve(String baseName, SymbolLookup lookup) {
        return baseName;
    }
}
