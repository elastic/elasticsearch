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
     * Current compatibility versions known at compile time
     *
     * <p>Some of our compatibility versions may be constructed at runtime, but in
     * many tests those will not be needed. This utility method returns only the current values for
     * compatibility versions defined at runtime, which is generally just those defined in the server
     * module.
     * @return Compatibility versions known at compile time.
     */
    public static CompatibilityVersions compileTimeCurrent() {
        return new CompatibilityVersions(TransportVersion.current());
    }

    /**
     * Random versions known at compile time
     *
     * <p>Like {@link #compileTimeCurrent()}, but with random valid versions.
     * @return Compatibility versions known at compile time.
     */
    public static CompatibilityVersions compileTimeRandom() {
        return new CompatibilityVersions(TransportVersionUtils.randomVersion());
    }
}
