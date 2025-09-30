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
    boolean supports(TransportVersion version);

    default boolean supportedLocally() {
        return supports(TransportVersion.current());
    }

    SupportedVersion SUPPORTED_ON_ALL_NODES = new SupportedVersion() {
        @Override
        public boolean supports(TransportVersion version) {
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
     */
    SupportedVersion UNDER_CONSTRUCTION = new SupportedVersion() {
        @Override
        public boolean supports(TransportVersion version) {
            return Build.current().isSnapshot();
        }

        @Override
        public String toString() {
            return "UnderConstruction";
        }
    };

    static SupportedVersion supportedOn(TransportVersion supportedVersion) {
        return new SupportedVersion() {
            @Override
            public boolean supports(TransportVersion version) {
                return version.supports(supportedVersion) || Build.current().isSnapshot();
            }

            @Override
            public String toString() {
                return "SupportedOn[" + supportedVersion + "]";
            }
        };
    }
}
