/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;

import java.io.File;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

/**
 * Checks the license declared in the metadata of a deb or rpm package, which is what package managers show to users,
 * as opposed to the license files installed by the package.
 */
public abstract class CheckPackageLicenseMetadataTask extends DefaultTask {

    private static final Pattern DEB_LICENSE = Pattern.compile("\\s*License: (.+)");

    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getPackageFile();

    @Input
    public abstract Property<PackageMetadata.Type> getPackageType();

    /**
     * The absolute path of {@code dpkg-deb} or {@code rpm}, matching {@link #getPackageType()}.
     */
    @Input
    public abstract Property<String> getExecutable();

    /**
     * The license the package metadata must declare, spelled as the package format expects it.
     */
    @Input
    public abstract Property<String> getExpectedLicense();

    @Inject
    protected abstract ExecOperations getExecOperations();

    @TaskAction
    public void checkLicenseMetadata() {
        PackageMetadata.Type type = getPackageType().get();
        File packageFile = getPackageFile().get().getAsFile();
        String output = PackageTools.run(getExecOperations(), commandLine(type, getExecutable().get(), packageFile));
        verifyLicense(type, output, getExpectedLicense().get(), packageFile.toString());
    }

    static List<String> commandLine(PackageMetadata.Type type, String executable, File packageFile) {
        return switch (type) {
            case DEB -> List.of(executable, "--info", packageFile.getAbsolutePath());
            case RPM -> List.of(executable, "-qp", "--queryformat", "%{License}", packageFile.getAbsolutePath());
        };
    }

    /**
     * Fails unless {@code output} of the {@link #commandLine} declares exactly the {@code expectedLicense}.
     *
     * @param output for deb the full {@code dpkg-deb --info} output, which must have a single {@code License:} line;
     *               for rpm just the license
     */
    static void verifyLicense(PackageMetadata.Type type, String output, String expectedLicense, String packageName) {
        switch (type) {
            case DEB -> {
                int count = 0;
                for (String line : output.split("\n")) {
                    Matcher matcher = DEB_LICENSE.matcher(line);
                    if (matcher.matches()) {
                        count++;
                        String actualLicense = matcher.group(1);
                        if (expectedLicense.equals(actualLicense) == false) {
                            throw new GradleException(
                                "expected license [" + expectedLicense + "] for package info but found [" + actualLicense + "]"
                            );
                        }
                    }
                }
                if (count == 0) {
                    throw new GradleException("expected license [" + expectedLicense + "] for package info but found none in:\n" + output);
                }
                if (count > 1) {
                    throw new GradleException("expected a single license for package info but found [" + count + "] in:\n" + output);
                }
            }
            case RPM -> {
                if (expectedLicense.equals(output) == false) {
                    throw new GradleException(
                        "expected license [" + expectedLicense + "] for [" + packageName + "] but was [" + output + "]"
                    );
                }
            }
        }
    }
}
