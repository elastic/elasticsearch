/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.plugins.Platforms;

public enum SupportedPlatformArchitectures {
    PLATFORM_INDEPENDENT(""),
    LINUX_ORIGINAL("linux-x86_64"),
    LINUX_ARM("linux-aarch64"),
    DARWIN_ORIGINAL("darwin-x86_64"),
    DARWIN_ARM("darwin-aarch64"),
    WINDOWS_ORIGINAL("windows-x86_64");

    private final String platformArchitecture;

    SupportedPlatformArchitectures(String platformArchitecture) {
        this.platformArchitecture = platformArchitecture;
    }

    SupportedPlatformArchitectures(String osName, String osArch) {
        platformArchitecture = Platforms.platformName(osName, osArch);
    }

    public String toString() {
        return platformArchitecture;
    }
}
