/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.core;

import java.util.function.Function;

/**
 * NB: Light-clone from Core library to keep JDBC driver independent.
 *
 * A enum representing versions of the REST API (particularly with regard to backwards compatibility).
 *
 * Only major versions are supported.
 */
public enum RestApiVersion {

    V_8(8),
    V_7(7);

    public final byte major;

    private static final RestApiVersion CURRENT = V_8;

    RestApiVersion(int major) {
        this.major = (byte) major;
    }

    public RestApiVersion previous() {
        return fromMajorVersion(major - 1);
    }

    public boolean matches(Function<RestApiVersion, Boolean> restApiVersionFunctions) {
        return restApiVersionFunctions.apply(this);
    }

    private static RestApiVersion fromMajorVersion(int majorVersion) {
        return valueOf("V_" + majorVersion);
    }

    public static RestApiVersion minimumSupported() {
        return current().previous();
    }

    public static RestApiVersion current() {
        return CURRENT;
    }

    public static Function<RestApiVersion, Boolean> equalTo(RestApiVersion restApiVersion) {
        return r -> r.major == restApiVersion.major;
    }

    public static Function<RestApiVersion, Boolean> onOrAfter(RestApiVersion restApiVersion) {
        return r -> r.major >= restApiVersion.major;
    }

}
