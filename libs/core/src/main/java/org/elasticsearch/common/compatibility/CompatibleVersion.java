/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compatibility;

public enum CompatibleVersion {

    V_8(8),
    V_7(7);

    // This needs to be aligned with Version.CURRENT
    public static final CompatibleVersion CURRENT = V_8;
    public byte major;

    CompatibleVersion(int major) {
        this.major = (byte) major;
    }

    public CompatibleVersion previousMajor() {
        return fromMajorVersion(major - 1);
    }

    public static CompatibleVersion fromMajorVersion(int majorVersion) {
        return valueOf("V_" + majorVersion);
    }

    public static CompatibleVersion minimumRestCompatibilityVersion() {
        return CURRENT.previousMajor();
    }
}
