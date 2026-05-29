/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.aws;

import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.between;

/// Specifies how the `disable_chunked_encoding` S3 client setting is configured - either `true` or `false` or omitted (default).
public enum ChunkedEncodingConfiguration {

    /// `disable_chunked_encoding: true`
    DISABLED {
        @Override
        Settings.Builder apply(Settings.Builder builder) {
            return builder.put(DISABLE_CHUNKED_ENCODING, "true");
        }
    },

    /// `disable_chunked_encoding: false`
    ENABLED {
        @Override
        Settings.Builder apply(Settings.Builder builder) {
            return builder.put(DISABLE_CHUNKED_ENCODING, "false");
        }
    },

    /// `disable_chunked_encoding: null` (i.e. setting omitted, use default behaviour)
    UNSET {
        @Override
        Settings.Builder apply(Settings.Builder builder) {
            return builder.putNull(DISABLE_CHUNKED_ENCODING);
        }
    };

    private static final String DISABLE_CHUNKED_ENCODING = "disable_chunked_encoding";

    abstract Settings.Builder apply(Settings.Builder builder);

    public Settings asSettings() {
        return apply(Settings.builder()).build();
    }

    public static Supplier<ChunkedEncodingConfiguration> randomSupplier() {
        return new RandomSupplier()::getChunkedEncodingConfiguration;
    }

    /** Lazy because randomness isn't available in static context */
    private static class RandomSupplier extends AtomicInteger {
        ChunkedEncodingConfiguration getChunkedEncodingConfiguration() {
            compareAndSet(0, between(1, 3));
            return ChunkedEncodingConfiguration.values()[get() - 1];
        }
    }
}
