/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.toolchain;

import org.gradle.jvm.toolchain.JavaToolchainResolver;
import org.gradle.jvm.toolchain.JvmVendorSpec;
import org.gradle.platform.Architecture;
import org.gradle.platform.OperatingSystem;

abstract class AbstractCustomJavaToolchainResolver implements JavaToolchainResolver {

    static String toOsString(OperatingSystem operatingSystem) {
        return toOsString(operatingSystem, null);
    }

    static String toOsString(OperatingSystem operatingSystem, JvmVendorSpec v) {
        return switch (operatingSystem) {
            case MAC_OS -> (v == null || v.equals(JvmVendorSpec.ADOPTIUM) == false) ? "macos" : "mac";
            case LINUX -> "linux";
            case WINDOWS -> "windows";
            default -> throw new UnsupportedOperationException("Operating system " + operatingSystem);
        };
    }

    static String toArchString(Architecture architecture) {
        return switch (architecture) {
            case X86_64 -> "x64";
            case AARCH64 -> "aarch64";
            case X86 -> "x86";
        };
    }

    protected static boolean anyVendorOr(JvmVendorSpec givenVendor, JvmVendorSpec expectedVendor) {
        return givenVendor.matches("any") || givenVendor.equals(expectedVendor);
    }
}
