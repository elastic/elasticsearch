/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.Version;

import static org.elasticsearch.Version.V_7_11_0;

/**
 * Utility class to enable gradual feature enabling, version-dependent.
 */
public final class VersionUtil {

    private VersionUtil() {}

    /**
     * Does the provided {@code version} support the unsigned_long type (PR#60050)?
     */
    public static boolean isUnsignedLongSupported(Version version) {
        return V_7_11_0.compareTo(version) <= 0;
    }
}
