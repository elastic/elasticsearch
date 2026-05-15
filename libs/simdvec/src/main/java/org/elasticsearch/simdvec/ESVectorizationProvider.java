/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.util.Constants;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;
import org.elasticsearch.simdvec.internal.vectorization.JdkFeatures;
import org.elasticsearch.simdvec.internal.vectorization.PanamaVectorConstants;

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

    abstract ESVectorUtilSupport getVectorUtilSupport();

    public abstract VectorScorerFactory getVectorScorerFactory();

    // visible for tests
    public static ESVectorizationProvider lookup(boolean allowPanama, boolean allowNative) {
        final int runtimeVersion = Runtime.version().feature();
        assert runtimeVersion >= 21;

        if (!allowPanama) {
            return new DefaultESVectorizationProvider();
        }

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
        ESVectorizationProvider.class.getModule().addReads(vectorMod.get());

        boolean nativeSupported = allowNative && NativeAccess.instance().getVectorSimilarityFunctions().isPresent();
        boolean supportsHeapSegments = JdkFeatures.SUPPORTS_HEAP_SEGMENTS;
        // nativeSupported is already logged by NativeAccess, and JDK version is readily inferred
        logger.info(
            String.format(
                Locale.ENGLISH,
                "Java vector incubator API enabled; uses preferredBitSize=%d",
                PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE
            )
        );

        if (nativeSupported && supportsHeapSegments) {
            return new Native22ESVectorizationProvider();
        } else if (supportsHeapSegments) {
            // no native support, but does support heap segments
            return new Panama22ESVectorizationProvider();
        } else {
            return new Panama21ESVectorizationProvider();
        }
    }

    private static Optional<Module> lookupVectorModule() {
        return Optional.ofNullable(ESVectorizationProvider.class.getModule().getLayer())
            .orElse(ModuleLayer.boot())
            .findModule("jdk.incubator.vector");
    }

    /** This static holder class prevents classloading deadlock. */
    private static final class Holder {
        private Holder() {}

        static final ESVectorizationProvider INSTANCE = lookup(true, true);
    }
}
