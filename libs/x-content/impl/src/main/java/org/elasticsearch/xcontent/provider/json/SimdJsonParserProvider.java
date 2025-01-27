/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import org.simdjson.SimdJsonParser;

import java.util.Objects;

public abstract class SimdJsonParserProvider {
    public static SimdJsonParserProvider getInstance() {
        return Objects.requireNonNull(
            SimdJsonParserProvider.Holder.INSTANCE,
            "call to getInstance() from subclass of VectorizationProvider"
        );
    }

    SimdJsonParserProvider() {}

    public abstract SimdJsonParser getParser();

    // visible for tests
    static SimdJsonParserProvider lookup(boolean testMode) {
        return new SimdJsonParserProvider() {
            @Override
            public SimdJsonParser getParser() {
                return null;
            }
        };
    }

    /** This static holder class prevents classloading deadlock. */
    private static final class Holder {
        private Holder() {}

        static final SimdJsonParserProvider INSTANCE = lookup(false);
    }
}
