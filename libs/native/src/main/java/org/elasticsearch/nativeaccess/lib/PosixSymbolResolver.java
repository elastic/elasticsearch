/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.foreign.ResolvedSymbol;
import org.elasticsearch.foreign.SymbolResolver;

import java.lang.foreign.SymbolLookup;

/**
 * Resolves {@code fstat64} to the {@code __fxstat} symbol on platforms (older glibc) that do not
 * export {@code fstat64} directly. All other symbols resolve by their exact name.
 */
public final class PosixSymbolResolver implements SymbolResolver {

    @Override
    public ResolvedSymbol resolve(String symbolName, SymbolLookup lookup) {
        if ("fstat64".equals(symbolName)) {
            var fstat64 = lookup.find("fstat64");
            if (fstat64.isPresent()) {
                return new ResolvedSymbol("fstat64", fstat64.get());
            }
            var fxstat = lookup.find("__fxstat").orElseThrow(() -> new UnsatisfiedLinkError("neither fstat64 nor __fxstat found"));
            return new ResolvedSymbol("__fxstat", fxstat);
        }
        return new ResolvedSymbol(
            symbolName,
            lookup.find(symbolName).orElseThrow(() -> new UnsatisfiedLinkError("Symbol not found: " + symbolName))
        );
    }
}
