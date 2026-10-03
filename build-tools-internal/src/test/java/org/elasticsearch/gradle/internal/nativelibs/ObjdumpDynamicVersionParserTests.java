/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.gradle.internal.nativelibs.LinuxSymbolVersion.Kind.GLIBC;
import static org.elasticsearch.gradle.internal.nativelibs.LinuxSymbolVersion.Kind.GLIBCXX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ObjdumpDynamicVersionParserTests {

    private static final String COMPATIBLE_OBJDUMP = """
        Version References:
          required from libstdc++.so.6:
            0x08922974 0x00 03 GLIBCXX_3.4
          required from libc.so.6:
            0x06969197 0x00 02 GLIBC_2.17
        """;

    private static final String INCOMPATIBLE_OBJDUMP = """
        Version References:
          required from libstdc++.so.6:
            0x0297f842 0x00 03 GLIBCXX_3.4.32
          required from libc.so.6:
            0x06969197 0x00 02 GLIBC_2.17
        """;

    @Test
    public void extractsHighestReferencedVersions() {
        Map<LinuxSymbolVersion.Kind, LinuxSymbolVersion> referenced = ObjdumpDynamicVersionParser.highestReferencedVersions(
            COMPATIBLE_OBJDUMP
        );
        assertEquals(new LinuxSymbolVersion(GLIBC, 2, 17, 0), referenced.get(GLIBC));
        assertEquals(new LinuxSymbolVersion(GLIBCXX, 3, 4, 0), referenced.get(GLIBCXX));
    }

    @Test
    public void selectsHighestWhenMultipleVersionsReferencedPerKind() {
        String objdump = """
            Version References:
              required from libstdc++.so.6:
                0x08922974 0x00 03 GLIBCXX_3.4
                0x08922975 0x00 04 GLIBCXX_3.4.19
                0x08922976 0x00 05 GLIBCXX_3.4.21
              required from libc.so.6:
                0x06969190 0x00 04 GLIBC_2.14.0
                0x06969197 0x00 02 GLIBC_2.17
                0x06969194 0x00 03 GLIBC_2.28
            """;
        Map<LinuxSymbolVersion.Kind, LinuxSymbolVersion> referenced = ObjdumpDynamicVersionParser.highestReferencedVersions(objdump);
        assertEquals(new LinuxSymbolVersion(GLIBC, 2, 28, 0), referenced.get(GLIBC));
        assertEquals(new LinuxSymbolVersion(GLIBCXX, 3, 4, 21), referenced.get(GLIBCXX));
    }

    @Test
    public void selectsHighestVersionTokenNotLastOnLine() {
        String objdump = "noise GLIBCXX_3.4.32 0x0297f842 0x00 03 GLIBCXX_3.4 more-noise";
        Map<LinuxSymbolVersion.Kind, LinuxSymbolVersion> referenced = ObjdumpDynamicVersionParser.highestReferencedVersions(objdump);
        assertEquals(new LinuxSymbolVersion(GLIBCXX, 3, 4, 32), referenced.get(GLIBCXX));

        objdump = "noise GLIBCXX_3.4.32\n0x0297f842 0x00 03 GLIBCXX_3.4\nmore-noise";
        referenced = ObjdumpDynamicVersionParser.highestReferencedVersions(objdump);
        assertEquals(new LinuxSymbolVersion(GLIBCXX, 3, 4, 32), referenced.get(GLIBCXX));

        objdump = "noise GLIBCXX_3.4.33\t0x0297f842 0x00 03 GLIBCXX_3.4\tmore-noise";
        referenced = ObjdumpDynamicVersionParser.highestReferencedVersions(objdump);
        assertEquals(new LinuxSymbolVersion(GLIBCXX, 3, 4, 33), referenced.get(GLIBCXX));

        objdump = "noise GLIBCXX_3.4.33\r0x0297f842 0x00 03 GLIBCXX_3.4.34\rmore-noise";
        referenced = ObjdumpDynamicVersionParser.highestReferencedVersions(objdump);
        assertEquals(new LinuxSymbolVersion(GLIBCXX, 3, 4, 34), referenced.get(GLIBCXX));
    }

    @Test
    public void returnsEmptyMapWhenNoVersionSymbols() {
        Map<LinuxSymbolVersion.Kind, LinuxSymbolVersion> referenced = ObjdumpDynamicVersionParser.highestReferencedVersions(
            "Dynamic Section:\n  NEEDED libz.so.1\n"
        );
        assertTrue(referenced.isEmpty());

        referenced = ObjdumpDynamicVersionParser.highestReferencedVersions(
            ""
        );
        assertTrue(referenced.isEmpty());
    }

    @Test
    public void reportsViolationsAgainstRhel8Policy() {
        LinuxSymbolVersion maxGlibc = new LinuxSymbolVersion(GLIBC, 2, 28, 0);
        LinuxSymbolVersion maxGlibcxx = new LinuxSymbolVersion(GLIBCXX, 3, 4, 25);

        List<String> compatibleViolations = ObjdumpDynamicVersionParser.findViolations(
            ObjdumpDynamicVersionParser.highestReferencedVersions(COMPATIBLE_OBJDUMP),
            maxGlibc,
            maxGlibcxx
        );
        assertTrue(compatibleViolations.isEmpty());

        List<String> incompatibleViolations = ObjdumpDynamicVersionParser.findViolations(
            ObjdumpDynamicVersionParser.highestReferencedVersions(INCOMPATIBLE_OBJDUMP),
            maxGlibc,
            maxGlibcxx
        );
        assertEquals(1, incompatibleViolations.size());
        assertTrue(incompatibleViolations.get(0).contains("GLIBCXX_3.4.32"));
    }

    @Test
    public void recognizesLinuxSharedLibraryPaths() {
        assertTrue(ObjdumpDynamicVersionParser.isLinuxSharedLibrary("platform/linux-aarch64/libvec.so"));
        assertTrue(ObjdumpDynamicVersionParser.isLinuxSharedLibrary("platform/linux-x64/libes_simdjson.so"));
        assertFalse(ObjdumpDynamicVersionParser.isLinuxSharedLibrary("platform/darwin-aarch64/libvec.dylib"));
        assertFalse(ObjdumpDynamicVersionParser.isLinuxSharedLibrary("platform/windows-x64/libzstd.dll"));
    }
}
