/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.info;

import org.gradle.api.JavaVersion;
import org.gradle.api.provider.Provider;

import java.io.File;

public class RuntimeJava {

    private final Provider<File> javahome;
    private final Provider<JavaVersion> javaVersion;
    private final boolean explicitlySet;
    private final String preReleaseType;
    private final Provider<String> vendorDetails;
    private final Integer buildNumber;

    RuntimeJava(Provider<File> javahome, Provider<JavaVersion> javaVersion, Provider<String> vendorDetails, boolean explicitlySet) {
        this(javahome, javaVersion, vendorDetails, explicitlySet, null, null);
    }

    RuntimeJava(
        Provider<File> javahome,
        Provider<JavaVersion> javaVersion,
        Provider<String> vendorDetails,
        boolean explicitlySet,
        String preReleaseType,
        Integer buildNumber
    ) {
        this.javahome = javahome;
        this.javaVersion = javaVersion;
        this.vendorDetails = vendorDetails;
        this.explicitlySet = explicitlySet;
        this.preReleaseType = preReleaseType;
        this.buildNumber = buildNumber;
    }

    public Provider<File> getJavahome() {
        return javahome;
    }

    public boolean isPreRelease() {
        return preReleaseType != null;
    }

    public Provider<JavaVersion> getJavaVersion() {
        return javaVersion;
    }

    public Provider<String> getVendorDetails() {
        return vendorDetails;
    }

    public boolean isExplicitlySet() {
        return explicitlySet;
    }

    public String getPreReleaseType() {
        return preReleaseType;
    }

    public Integer getBuildNumber() {
        return buildNumber;
    }
}
