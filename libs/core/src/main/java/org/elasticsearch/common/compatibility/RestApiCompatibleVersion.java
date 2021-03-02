/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compatibility;

import java.util.Collection;
import java.util.function.Function;

/**
 * A enum representing versions which are used by a REST Compatible API.
 * A CURRENT instance, represents a major Version.CURRENT from server module.
 *
 * Only major versions are supported.
 */
public enum RestApiCompatibleVersion {

    V_8(8),
    V_7(7);

    public final byte major;
    private static final RestApiCompatibleVersion CURRENT = V_8;

    RestApiCompatibleVersion(int major) {
        this.major = (byte) major;
    }

    public RestApiCompatibleVersion previousMajor() {
        return fromMajorVersion(major - 1);
    }

    public boolean matches(Collection<Function<RestApiCompatibleVersion, Boolean>> restApiCompatibleVersionFunctions){
        return restApiCompatibleVersionFunctions.stream().anyMatch(r -> r.apply(this));
    }

    public static RestApiCompatibleVersion fromMajorVersion(int majorVersion) {
        return valueOf("V_" + majorVersion);
    }

    public static RestApiCompatibleVersion minimumSupported() {
        return currentVersion().previousMajor();
    }

    public static RestApiCompatibleVersion currentVersion() {
        return CURRENT;
    };

    public static Function<RestApiCompatibleVersion, Boolean> equalTo(RestApiCompatibleVersion restApiCompatibleVersion) {
        return r -> r.major == restApiCompatibleVersion.major;
    }

    public static Function<RestApiCompatibleVersion, Boolean> onOrAfter(RestApiCompatibleVersion restApiCompatibleVersion) {
        return r -> r.major >= restApiCompatibleVersion.major;
    }
}
