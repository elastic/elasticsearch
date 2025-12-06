/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

public enum Platform {
    LINUX,
    MACOS,
    WINDOWS;

    private static final Platform current = findCurrent();

    private static Platform findCurrent() {
        String os = System.getProperty("os.name");
        if (os.startsWith("Linux")) {
            return LINUX;
        } else if (os.startsWith("Mac OS")) {
            return MACOS;
        } else if (os.startsWith("Windows")) {
            return WINDOWS;
        } else {
            throw new AssertionError("Unsupported platform [" + os + "]");
        }
    }

    public boolean isCurrent() {
        return this == current;
    }
}
