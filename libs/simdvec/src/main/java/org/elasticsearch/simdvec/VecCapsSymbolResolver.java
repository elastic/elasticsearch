/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.foreign.ResolvedSymbol;
import org.elasticsearch.foreign.SymbolResolver;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.foreign.SymbolLookup;

final class VecCapsSymbolResolver implements SymbolResolver {

    private static final Logger logger = LogManager.getLogger(VecCapsSymbolResolver.class);

    /**
     * Native functions in the native simdvec library can have multiple implementations, one for each "capability level".
     * <p>
     * Functions for the base ("1") level are exposed with a simple function name (e.g. "vec_doti7u")
     * Functions for the more advanced levels (2, 3, ...) are exported with a name "decorated" by adding the capability level as
     * a suffix: if the capability level is N, the suffix will be "_N" (e.g. "vec_doti7u_2").
     *
     * <p>
     * This method resolves to the function with the highest capability level exported by the native library by performing fallback lookups:
     * starting from the supported capability level N, it looks up function_N, function_{N-1}... function.
     *
     * @param functionName the base function name, as exported by the native library
     * @return             a {@link ResolvedSymbol} with the resolved named and address of the native function
     */
    @Override
    public ResolvedSymbol resolve(String functionName, SymbolLookup lookup) {
        int capability = VecCaps.caps();
        for (int caps = capability; caps > 0; --caps) {
            var suffix = caps > 1 ? "_" + caps : "";
            var fullFunctionName = functionName + suffix;
            logger.trace("Lookup for {}", fullFunctionName);
            var function = lookup.find(functionName + suffix).orElse(null);
            if (function != null) {
                logger.debug("Binding {}", fullFunctionName);
                return new ResolvedSymbol(fullFunctionName, function);
            }
        }
        throw new LinkageError("Native function [" + functionName + "] could not be found");
    }
}
