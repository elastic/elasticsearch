/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

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

    RestApiVersion(int major) {
        this.major = (byte) major;
    }

    public RestApiVersion previous() {
        return fromMajorVersion(major - 1);
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

}
