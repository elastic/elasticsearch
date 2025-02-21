/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.util.function.Predicate;

/**
 * A enum representing versions of the REST API (particularly with regard to backwards compatibility).
 *
 * Only major versions are supported.
 */
public enum RestApiVersion {

    V_9(9),

    V_8(8);

    public final byte major;

    private static final RestApiVersion CURRENT = V_9;
    private static final RestApiVersion PREVIOUS = V_8;

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
            case V_9 -> r -> r.major == V_9.major;
            case V_8 -> r -> r.major == V_8.major;
        };
    }

    public static Predicate<RestApiVersion> onOrAfter(RestApiVersion restApiVersion) {
        return switch (restApiVersion) {
            case V_9 -> r -> r.major >= V_9.major;
            case V_8 -> r -> r.major >= V_8.major;
        };
    }

    public static RestApiVersion forMajorIfKnown(int major) {
        return switch (major) {
            case 8 -> V_8;
            case 9 -> V_9;
            default -> null;
        };
    }
}
