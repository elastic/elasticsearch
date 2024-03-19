/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.version;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.Map;

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
        return new CompatibilityVersions(TransportVersion.current(), Map.of());
    }

    /**
     * Random versions of values that can be chosen statically (as opposed to those
     * that are loaded from plugins at startup time).
     *
     * <p>Like {@link #staticCurrent()}, but with random valid versions.
     * @return Random valid compatibility versions
     */
    public static CompatibilityVersions staticRandom() {
        return new CompatibilityVersions(TransportVersionUtils.randomVersion(), Map.of());
    }

    public static CompatibilityVersions fakeSystemIndicesRandom() {
        return new CompatibilityVersions(
            TransportVersionUtils.randomVersion(),
            ESTestCase.randomMap(
                0,
                3,
                () -> Tuple.tuple(
                    "." + ESTestCase.randomAlphaOfLength(5),
                    new SystemIndexDescriptor.MappingsVersion(ESTestCase.randomInt(20), ESTestCase.randomInt())
                )
            )
        );
    }
}
