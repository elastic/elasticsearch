/*
 * @notice
 *
 * Copyright 2021-2024 The simdjson-java contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Based on a modification of https://github.com/simdjson/simdjson-java,
 * licensed under the Apache License 2.0.
 */

package org.elasticsearch.simdjson;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.foreign.LibraryProvider;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdjson.internal.SimdJsonLibrary;

import java.util.Optional;

/**
 * Bootstrap class that wires a read-edge from this module to {@code jdk.incubator.vector}
 * before any vector class is loaded, and exposes the configured {@link #BYTE_SPECIES}.
 *
 * <p>The vector species is selected via the {@code es.simdjson.species} system property:
 * {@code preferred} (default), {@code 128}, {@code 256}, or {@code 512}.
 *
 * <p><strong>Class-init ordering:</strong> the static initializer first adds the read-edge
 * to {@code jdk.incubator.vector}, then resolves the vector species. All classes that use
 * vector types must trigger {@code SimdJsonSupport} initialisation first (e.g. by reading
 * {@link #BYTE_SPECIES} or calling {@link #isSupported()}).
 */
public final class SimdJsonSupport {

    static final Logger logger = LogManager.getLogger(SimdJsonSupport.class);

    /**
     * The preferred {@link VectorSpecies} for SIMD operations, or {@code null} if
     * {@code jdk.incubator.vector} is not available. Public for access from the
     * {@code internal} package within this module; not exported to external modules.
     */
    public static final VectorSpecies<Byte> BYTE_SPECIES;

    /**
     * The singleton native library instance, or {@code null} if the native library
     * is not supported or failed to load. Public for access from the {@code internal}
     * package within this module; not exported to external modules.
     */
    public static final SimdJsonLibrary LIB = SimdJsonSupport.tryLoad();

    static {
        Optional<Module> vec = Optional.ofNullable(SimdJsonSupport.class.getModule().getLayer())
            .orElse(ModuleLayer.boot())
            .findModule("jdk.incubator.vector");
        if (vec.isPresent()) {
            SimdJsonSupport.class.getModule().addReads(vec.get());
            BYTE_SPECIES = selectSpecies();
        } else {
            BYTE_SPECIES = null;
        }
    }

    private SimdJsonSupport() {}

    /**
     * Returns {@code true} if simdjson is fully operational: the native C++ library is
     * loaded and the incubating vector API is available. This is the single availability
     * check that callers should use before constructing a {@link SimdJsonParser}.
     */
    public static boolean isSupported() {
        return LIB != null && BYTE_SPECIES != null;
    }

    private static VectorSpecies<Byte> selectSpecies() {
        String species = System.getProperty("tests.vectorsize", "preferred");
        var s = switch (species) {
            case "preferred" -> ByteVector.SPECIES_PREFERRED;
            case "512" -> ByteVector.SPECIES_512;
            case "256" -> ByteVector.SPECIES_256;
            case "128" -> ByteVector.SPECIES_128;
            default -> throw new IllegalArgumentException("Unsupported es.simdjson.species: " + species);
        };
        logger.info("simdjson using " + s);
        return s;
    }

    /**
     * Loads the native simdjson library via {@link LibraryProvider}, or returns null if this
     * host CPU/OS does not support it.
     */
    static SimdJsonLibrary tryLoad() {
        if (isNativeLibSupported() && checkEnableSystemProperty()) {
            try {
                var lib = LibraryProvider.lookupLibrary(SimdJsonLibrary.class);
                if (lib != null) {
                    logger.info("Loaded simdjson native library");
                    return lib;
                }
            } catch (UnsatisfiedLinkError e) {
                logger.info("simdjson native library not available: {}", e.getMessage());
            } catch (Exception e) {
                logger.warn("Failed to instantiate simdjson native library", e);
            }
        }
        return null;
    }

    static boolean isNativeLibSupported() {
        return isMacOrLinuxAarch64() || isLinuxAmd64();
    }

    /**
     * Returns true iff the architecture is x64 (amd64) and the OS Linux (the OS we currently support for the native lib).
     */
    static boolean isLinuxAmd64() {
        String name = System.getProperty("os.name");
        return (name.startsWith("Linux")) && System.getProperty("os.arch").equals("amd64");
    }

    /** Returns true iff the OS is Mac or Linux, and the architecture is aarch64. */
    static boolean isMacOrLinuxAarch64() {
        String name = System.getProperty("os.name");
        return (name.startsWith("Mac") || name.startsWith("Linux")) && System.getProperty("os.arch").equals("aarch64");
    }

    /** -Dorg.elasticsearch.nativeaccess.enableVectorLibrary=false to disable.*/
    static final String ENABLE_SIMD_JSON_LIBRARY = "org.elasticsearch.simdjson.enableSimdJsonLibrary";

    @SuppressForbidden(
        reason = "TODO Deprecate any lenient usage of Boolean#parseBoolean https://github.com/elastic/elasticsearch/issues/128993"
    )
    static boolean checkEnableSystemProperty() {
        return Optional.ofNullable(System.getProperty(ENABLE_SIMD_JSON_LIBRARY)).map(Boolean::valueOf).orElse(Boolean.TRUE);
    }
}
