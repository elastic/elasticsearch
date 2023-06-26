/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.util.function.Predicate;

/**
 * A enum representing versions of the REST API (particularly with regard to backwards compatibility).
 *
 * Only major versions are supported.
 */
public enum RestApiVersion {

    V_8(8),
    V_7(7);

    public final byte major;

    private static final RestApiVersion CURRENT = V_8;
    private static final RestApiVersion PREVIOUS = V_7;

    RestApiVersion(int major) {
        this.major = (byte) major;
    }

    public boolean matches(Predicate<RestApiVersion> restApiVersionFunctions) {
        return restApiVersionFunctions.test(this);
    }

    public static RestApiVersion current() {
        return CURRENT;
    }

    public static RestApiVersion previous() {
        return PREVIOUS;
    }

    public static RestApiVersion minimumSupported() {
        return PREVIOUS;
    }

    public static Predicate<RestApiVersion> equalTo(RestApiVersion restApiVersion) {
        return switch (restApiVersion) {
            case V_8 -> r -> r.major == V_8.major;
            case V_7 -> r -> r.major == V_7.major;
        };
    }

    public static Predicate<RestApiVersion> onOrAfter(RestApiVersion restApiVersion) {
        return switch (restApiVersion) {
            case V_8 -> r -> r.major >= V_8.major;
            case V_7 -> r -> r.major >= V_7.major;
        };
    }

}
