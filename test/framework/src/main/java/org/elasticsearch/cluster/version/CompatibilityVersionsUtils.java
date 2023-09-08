/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.version;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;

public class CompatibilityVersionsUtils {

    /**
     * Current compatibility versions that can be determined statically
     *
     * <p>Some of our compatibility versions may be constructed at runtime, but in
     * many tests those will not be needed. This utility method returns only the "current"
     * values for statically defined versions, like {@link TransportVersion#current()}.
     *
     * @return Compatibility versions known at compile time.
     */
    public static CompatibilityVersions staticCurrent() {
        return new CompatibilityVersions(TransportVersion.current());
    }

    /**
     * Random versions of values that can be chosen statically (as opposed to those
     * that are loaded from plugins at startup time).
     *
     * <p>Like {@link #staticCurrent()}, but with random valid versions.
     * @return Random valid compatibility versions
     */
    public static CompatibilityVersions staticRandom() {
        return new CompatibilityVersions(TransportVersionUtils.randomVersion());
    }
}
