/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.parsers;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Objects;

/**
 * Holds the configured {@link VectorSpecies} for simdjson string parsing.
 *
 * <p>Initialized by {@link org.elasticsearch.simdjson.SimdJsonSupport}'s static initializer
 * after that module adds its read edge to {@code jdk.incubator.vector}. Callers such as
 * {@link StringParser} must not load until that bootstrap has run (typically by referencing
 * {@link org.elasticsearch.simdjson.SimdJsonSupport#isSupported()} first).
 */
public final class SimdJsonVectorSupport {

    private static final Logger logger = LogManager.getLogger(SimdJsonVectorSupport.class);

    private static VectorSpecies<Byte> byteSpecies;

    private SimdJsonVectorSupport() {}

    public static void init() {
        byteSpecies = selectSpecies();
    }

    public static boolean isAvailable() {
        return byteSpecies != null;
    }

    public static VectorSpecies<Byte> byteSpecies() {
        return Objects.requireNonNull(
            byteSpecies,
            "vector support not initialized; SimdJsonSupport must be initialized before StringParser"
        );
    }

    public static int vectorByteSize() {
        return byteSpecies == null ? 0 : byteSpecies.vectorByteSize();
    }

    private static VectorSpecies<Byte> selectSpecies() {
        String species = System.getProperty("tests.vectorsize", "preferred");
        VectorSpecies<Byte> selected = switch (species) {
            case "preferred" -> ByteVector.SPECIES_PREFERRED;
            case "512" -> ByteVector.SPECIES_512;
            case "256" -> ByteVector.SPECIES_256;
            case "128" -> ByteVector.SPECIES_128;
            default -> throw new IllegalArgumentException("Unsupported es.simdjson.species: " + species);
        };
        logger.info("simdjson using " + selected);
        return selected;
    }
}
