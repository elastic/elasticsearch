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
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExtractPackageTaskTests {

    private static final File PACKAGE = new File("/build/distributions/elasticsearch.pkg");
    private static final File DESTINATION = new File("/build/extracted");
    private static final File RPM_DATABASE = new File("/tmp/rpm-database");

    @Test
    public void extractsDebWithDpkgDeb() {
        assertEquals(
            List.of("/usr/bin/dpkg-deb", "-x", PACKAGE.getAbsolutePath(), DESTINATION.getAbsolutePath()),
            ExtractPackageTask.commandLine(Type.DEB, "/usr/bin/dpkg-deb", PACKAGE, DESTINATION, RPM_DATABASE)
        );
    }

    /**
     * The rpm is installed into a throw-away database, relocated into the destination, without running its scripts or
     * resolving its dependencies, so it can be extracted on any host that has the rpm tool.
     */
    @Test
    public void extractsRpmByRelocatedInstallIntoThrowAwayDatabase() {
        assertEquals(
            List.of(
                "/usr/bin/rpm",
                "--badreloc",
                "--ignorearch",
                "--ignoreos",
                "--nodeps",
                "--noscripts",
                "--notriggers",
                "--nosignature",
                "--dbpath",
                RPM_DATABASE.getAbsolutePath(),
                "--relocate",
                "/=" + DESTINATION.getAbsolutePath(),
                "-i",
                PACKAGE.getAbsolutePath()
            ),
            ExtractPackageTask.commandLine(Type.RPM, "/usr/bin/rpm", PACKAGE, DESTINATION, RPM_DATABASE)
        );
    }
}
