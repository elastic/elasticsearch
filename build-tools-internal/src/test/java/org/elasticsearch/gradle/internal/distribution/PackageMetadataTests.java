/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.internal.distribution.PackageMetadata.Type;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class PackageMetadataTests {

    private static final PackageMetadata DEB = new PackageMetadata(Type.DEB, Architecture.X64);
    private static final PackageMetadata AARCH64_DEB = new PackageMetadata(Type.DEB, Architecture.AARCH64);
    private static final PackageMetadata RPM = new PackageMetadata(Type.RPM, Architecture.X64);
    private static final PackageMetadata AARCH64_RPM = new PackageMetadata(Type.RPM, Architecture.AARCH64);

    @Test
    public void parsesScriptIdentifiers() {
        assertEquals(DEB, PackageMetadata.of("deb", "x64"));
        assertEquals(AARCH64_DEB, PackageMetadata.of("deb", "aarch64"));
        assertEquals(RPM, PackageMetadata.of("rpm", "x64"));
        assertEquals(AARCH64_RPM, PackageMetadata.of("rpm", "aarch64"));
        assertThrows(IllegalArgumentException.class, () -> PackageMetadata.of("tar", "x64"));
        assertThrows(IllegalArgumentException.class, () -> PackageMetadata.of("deb", "x86_64"));
    }

    @Test
    public void projectNamesRoundTrip() {
        assertEquals("deb", DEB.projectName());
        assertEquals("aarch64-deb", AARCH64_DEB.projectName());
        assertEquals("rpm", RPM.projectName());
        assertEquals("aarch64-rpm", AARCH64_RPM.projectName());
        for (PackageMetadata metadata : List.of(DEB, AARCH64_DEB, RPM, AARCH64_RPM)) {
            assertEquals(metadata, PackageMetadata.fromProjectName(metadata.projectName()));
        }
        assertThrows(IllegalArgumentException.class, () -> PackageMetadata.fromProjectName("aarch64-tar"));
    }

    @Test
    public void taskNames() {
        assertEquals("buildDeb", DEB.buildTaskName());
        assertEquals("buildAarch64Deb", AARCH64_DEB.buildTaskName());
        assertEquals("buildRpm", RPM.buildTaskName());
        assertEquals("buildAarch64Rpm", AARCH64_RPM.buildTaskName());
        assertEquals("processDebFiles", Type.DEB.processFilesTaskName());
        assertEquals("processRpmFiles", Type.RPM.processFilesTaskName());
    }

    @Test
    public void envFile() {
        assertEquals("/etc/default/elasticsearch", Type.DEB.envFile());
        assertEquals("/etc/sysconfig/elasticsearch", Type.RPM.envFile());
    }

    @Test
    public void packageArchitecture() {
        assertEquals("amd64", DEB.packageArchitecture());
        assertEquals("arm64", AARCH64_DEB.packageArchitecture());
        assertEquals("X86_64", RPM.packageArchitecture());
        assertEquals("aarch64", AARCH64_RPM.packageArchitecture());
    }

    @Test
    public void packageVersion() {
        assertEquals("9.3.1~SNAPSHOT", DEB.packageVersion("9.3.1-SNAPSHOT"));
        assertEquals("9.3.1_SNAPSHOT", RPM.packageVersion("9.3.1-SNAPSHOT"));
        assertEquals("9.3.1", RPM.packageVersion("9.3.1"));
    }

    @Test
    public void license() {
        assertEquals("Elastic-License", AARCH64_DEB.license());
        assertEquals("Elastic License", AARCH64_RPM.license());
    }
}
