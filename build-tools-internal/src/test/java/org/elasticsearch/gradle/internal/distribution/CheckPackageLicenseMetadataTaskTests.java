/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.internal.distribution.PackageMetadata.Type;
import org.gradle.api.GradleException;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class CheckPackageLicenseMetadataTaskTests {

    private static final File PACKAGE = new File("/build/distributions/elasticsearch.pkg");

    private static final String DEB_INFO = """
         new Debian package, version 2.0.
         size 123 bytes: control archive=456 bytes.
         Package: elasticsearch
         Version: 9.6.0~SNAPSHOT
         License: Elastic-License
         Architecture: amd64
        """;

    @Test
    public void queriesLicenseMetadata() {
        assertEquals(
            List.of("/usr/bin/dpkg-deb", "--info", PACKAGE.getAbsolutePath()),
            CheckPackageLicenseMetadataTask.commandLine(Type.DEB, "/usr/bin/dpkg-deb", PACKAGE)
        );
        assertEquals(
            List.of("/usr/bin/rpm", "-qp", "--queryformat", "%{License}", PACKAGE.getAbsolutePath()),
            CheckPackageLicenseMetadataTask.commandLine(Type.RPM, "/usr/bin/rpm", PACKAGE)
        );
    }

    @Test
    public void acceptsSingleExpectedDebLicense() {
        CheckPackageLicenseMetadataTask.verifyLicense(Type.DEB, DEB_INFO, "Elastic-License", "elasticsearch.deb");
    }

    @Test
    public void rejectsUnexpectedDebLicense() {
        GradleException e = assertThrows(
            GradleException.class,
            () -> CheckPackageLicenseMetadataTask.verifyLicense(Type.DEB, DEB_INFO, "Apache-2.0", "elasticsearch.deb")
        );
        assertEquals("expected license [Apache-2.0] for package info but found [Elastic-License]", e.getMessage());
    }

    @Test
    public void rejectsDebInfoWithoutLicense() {
        String info = DEB_INFO.replace(" License: Elastic-License\n", "");
        GradleException e = assertThrows(
            GradleException.class,
            () -> CheckPackageLicenseMetadataTask.verifyLicense(Type.DEB, info, "Elastic-License", "elasticsearch.deb")
        );
        assertEquals("expected license [Elastic-License] for package info but found none in:\n" + info, e.getMessage());
    }

    @Test
    public void rejectsDebInfoWithMultipleLicenses() {
        String info = DEB_INFO + " License: Elastic-License\n";
        GradleException e = assertThrows(
            GradleException.class,
            () -> CheckPackageLicenseMetadataTask.verifyLicense(Type.DEB, info, "Elastic-License", "elasticsearch.deb")
        );
        assertEquals("expected a single license for package info but found [2] in:\n" + info, e.getMessage());
    }

    @Test
    public void comparesRpmLicenseExactly() {
        CheckPackageLicenseMetadataTask.verifyLicense(Type.RPM, "Elastic License", "Elastic License", "elasticsearch.rpm");
        GradleException e = assertThrows(
            GradleException.class,
            () -> CheckPackageLicenseMetadataTask.verifyLicense(Type.RPM, "Elastic-License", "Elastic License", "elasticsearch.rpm")
        );
        assertEquals("expected license [Elastic License] for [elasticsearch.rpm] but was [Elastic-License]", e.getMessage());
    }
}
