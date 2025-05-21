/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Constants;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

public abstract class ESVectorizationProvider {

    protected static final Logger logger = LogManager.getLogger(ESVectorizationProvider.class);

    public static ESVectorizationProvider getInstance() {
        return Objects.requireNonNull(
            ESVectorizationProvider.Holder.INSTANCE,
            "call to getInstance() from subclass of VectorizationProvider"
        );
    }

    ESVectorizationProvider() {}

    public abstract ESVectorUtilSupport getVectorUtilSupport();

    /** Create a new {@link ES91OSQVectorsScorer} for the given {@link IndexInput}. */
    public abstract ES91OSQVectorsScorer newES91OSQVectorsScorer(IndexInput input, int dimension) throws IOException;

    // visible for tests
    static ESVectorizationProvider lookup(boolean testMode) {
        final int runtimeVersion = Runtime.version().feature();
        assert runtimeVersion >= 21;
        if (runtimeVersion <= 24) {
            // only use vector module with Hotspot VM
            if (Constants.IS_HOTSPOT_VM == false) {
                logger.warn("Java runtime is not using Hotspot VM; Java vector incubator API can't be enabled.");
                return new DefaultESVectorizationProvider();
            }
            // is the incubator module present and readable (JVM providers may to exclude them or it is
            // build with jlink)
            final var vectorMod = lookupVectorModule();
            if (vectorMod.isEmpty()) {
                logger.warn(
                    "Java vector incubator module is not readable. "
                        + "For optimal vector performance, pass '--add-modules jdk.incubator.vector' to enable Vector API."
                );
                return new DefaultESVectorizationProvider();
            }
            vectorMod.ifPresent(ESVectorizationProvider.class.getModule()::addReads);
            var impl = new PanamaESVectorizationProvider();
            logger.info(
                String.format(
                    Locale.ENGLISH,
                    "Java vector incubator API enabled; uses preferredBitSize=%d",
                    PanamaESVectorUtilSupport.VECTOR_BITSIZE
                )
            );
            return impl;
        } else {
            logger.warn(
                "You are running with unsupported Java "
                    + runtimeVersion
                    + ". To make full use of the Vector API, please update Elasticsearch."
            );
        }
        return new DefaultESVectorizationProvider();
    }

    private static Optional<Module> lookupVectorModule() {
        return Optional.ofNullable(ESVectorizationProvider.class.getModule().getLayer())
            .orElse(ModuleLayer.boot())
            .findModule("jdk.incubator.vector");
    }

    /** This static holder class prevents classloading deadlock. */
    private static final class Holder {
        private Holder() {}

        static final ESVectorizationProvider INSTANCE = lookup(false);
    }
}
