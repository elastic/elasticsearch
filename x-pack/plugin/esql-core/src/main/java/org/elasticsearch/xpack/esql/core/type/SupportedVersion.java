/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;

public interface SupportedVersion {
    boolean supportedOn(TransportVersion version, boolean currentBuildIsSnapshot);

    default boolean supportedLocally() {
        return supportedOn(TransportVersion.current(), Build.current().isSnapshot());
    }

    SupportedVersion SUPPORTED_ON_ALL_NODES = new SupportedVersion() {
        @Override
        public boolean supportedOn(TransportVersion version, boolean currentBuildIsSnapshot) {
            return true;
        }

        @Override
        public String toString() {
            return "SupportedOnAllVersions";
        }
    };

    /**
     * Types that are actively being built. These types are
     * <ul>
     *     <li>Not returned from Elasticsearch on release builds.</li>
     *     <li>Not included in generated documentation</li>
     *     <li>
     *         Not tested by {@code ErrorsForCasesWithoutExamplesTestCase} subclasses.
     *         When a function supports a type it includes a test case in its subclass
     *         of {@code AbstractFunctionTestCase}. If a function does not support.
     *         them like {@code TO_STRING} then the tests won't notice. See class javadoc
     *         for instructions on adding new types, but that usually involves adding support
     *         for that type to a handful of functions. Once you've done that you should be
     *         able to remove your new type from UNDER_CONSTRUCTION and update a few error
     *         messages.
     *     </li>
     * </ul>
     * <p>
     *     Snapshot builds treat these as always supported so that we can write tests before actually
     *     turning on the support for the type. Mixed/multi cluster tests with older nodes have to be
     *     skipped based on capabilites, as always.
     */
    // We used to have a feature-flag based override, so that in-development types could be
    // turned on for testing in release builds. If needed, it's fine to bring this back, but we
    // need to make sure that other checks for types being under construction are also overridden.
    // Check usage of this constant to be sure.
    SupportedVersion UNDER_CONSTRUCTION = new SupportedVersion() {
        @Override
        public boolean supportedOn(TransportVersion version, boolean currentBuildIsSnapshot) {
            return currentBuildIsSnapshot;
        }

        @Override
        public String toString() {
            return "UnderConstruction";
        }
    };

    /**
     * Types that are supported starting with the given version.
     * <p>
     *     Snapshot builds treat these as always supported, so that any existing tests using them
     *     continue to work. Otherwise, we'd have to update bwc tests to skip older versions based
     *     on a capability check, which can be error-prone and risks turning off an unrelated bwc test.
     */
    static SupportedVersion supportedSince(TransportVersion supportedVersion) {
        return new SupportedVersion() {
            @Override
            public boolean supportedOn(TransportVersion version, boolean currentBuildIsSnapshot) {
                return version.supports(supportedVersion) || currentBuildIsSnapshot;
            }

            @Override
            public String toString() {
                return "SupportedOn[" + supportedVersion + "]";
            }
        };
    }
}
