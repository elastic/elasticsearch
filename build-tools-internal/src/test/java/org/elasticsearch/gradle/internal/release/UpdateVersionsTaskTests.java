/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Node;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.printer.lexicalpreservation.LexicalPreservingPrinter;

import org.elasticsearch.gradle.Version;
import org.junit.Test;

import java.io.StringWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class UpdateVersionsTaskTests {

    @Test
    public void addVersion_versionExists() {
        final String versionJava = """
            public class Version {
                public static final Version V_8_10_0 = new Version(8_10_00_99);
                public static final Version V_8_10_1 = new Version(8_10_01_99);
                public static final Version V_8_11_0 = new Version(8_11_00_99);
                public static final Version CURRENT = V_8_11_0;
            }""";

        CompilationUnit unit = StaticJavaParser.parse(versionJava);

        var newUnit = UpdateVersionsTask.addVersionConstant(unit, Version.fromString("8.10.1"), false);
        assertThat(newUnit.isPresent(), is(false));
    }

    @Test
    public void addVersion_oldVersion() {
        final String versionJava = """
            public class Version {
                public static final Version V_8_10_0 = new Version(8_10_00_99);
                public static final Version V_8_10_1 = new Version(8_10_01_99);
                public static final Version V_8_11_0 = new Version(8_11_00_99);
                public static final Version CURRENT = V_8_11_0;
            }""";
        final String updatedVersionJava = """
            public class Version {

                public static final Version V_8_10_0 = new Version(8_10_00_99);

                public static final Version V_8_10_1 = new Version(8_10_01_99);

                public static final Version V_8_10_2 = new Version(8_10_02_99);

                public static final Version V_8_11_0 = new Version(8_11_00_99);

                public static final Version CURRENT = V_8_11_0;
            }
            """;

        CompilationUnit unit = StaticJavaParser.parse(versionJava);

        UpdateVersionsTask.addVersionConstant(unit, Version.fromString("8.10.2"), false);

        assertThat(unit, hasToString(updatedVersionJava));
    }

    @Test
    public void addVersion_newVersion_current() {
        final String versionJava = """
            public class Version {
                public static final Version V_8_10_0 = new Version(8_10_00_99);
                public static final Version V_8_10_1 = new Version(8_10_01_99);
                public static final Version V_8_11_0 = new Version(8_11_00_99);
                public static final Version CURRENT = V_8_11_0;
            }""";
        final String updatedVersionJava = """
            public class Version {

                public static final Version V_8_10_0 = new Version(8_10_00_99);

                public static final Version V_8_10_1 = new Version(8_10_01_99);

                public static final Version V_8_11_0 = new Version(8_11_00_99);

                public static final Version V_8_11_1 = new Version(8_11_01_99);

                public static final Version CURRENT = V_8_11_1;
            }
            """;

        CompilationUnit unit = StaticJavaParser.parse(versionJava);

        UpdateVersionsTask.addVersionConstant(unit, Version.fromString("8.11.1"), true);

        assertThat(unit, hasToString(updatedVersionJava));
    }

    @Test
    public void removeVersion_versionDoesntExist() {
        final String versionJava = """
            public class Version {
                public static final Version V_8_10_0 = new Version(8_10_00_99);
                public static final Version V_8_10_1 = new Version(8_10_01_99);
                public static final Version V_8_11_0 = new Version(8_11_00_99);
                public static final Version CURRENT = V_8_11_0;
            }""";

        CompilationUnit unit = StaticJavaParser.parse(versionJava);

        var newUnit = UpdateVersionsTask.removeVersionConstant(unit, Version.fromString("8.10.2"));
        assertThat(newUnit.isPresent(), is(false));
    }

    @Test
    public void removeVersion_versionIsCurrent() {
        final String versionJava = """
            public class Version {
                public static final Version V_8_10_0 = new Version(8_10_00_99);
                public static final Version V_8_10_1 = new Version(8_10_01_99);
                public static final Version V_8_11_0 = new Version(8_11_00_99);
                public static final Version CURRENT = V_8_11_0;
            }""";

        CompilationUnit unit = StaticJavaParser.parse(versionJava);

        var ex = assertThrows(
            IllegalArgumentException.class,
            () -> UpdateVersionsTask.removeVersionConstant(unit, Version.fromString("8.11.0"))
        );
        assertThat(ex.getMessage(), equalTo("Cannot remove version [8.11.0], it is referenced by CURRENT"));
    }

    @Test
    public void removeVersion() {
        final String versionJava = """
            public class Version {
                public static final Version V_8_10_0 = new Version(8_10_00_99);
                public static final Version V_8_10_1 = new Version(8_10_01_99);
                public static final Version V_8_11_0 = new Version(8_11_00_99);
                public static final Version CURRENT = V_8_11_0;
            }""";
        final String updatedVersionJava = """
            public class Version {

                public static final Version V_8_10_0 = new Version(8_10_00_99);

                public static final Version V_8_11_0 = new Version(8_11_00_99);

                public static final Version CURRENT = V_8_11_0;
            }
            """;

        CompilationUnit unit = StaticJavaParser.parse(versionJava);

        UpdateVersionsTask.removeVersionConstant(unit, Version.fromString("8.10.1"));

        assertThat(unit, hasToString(updatedVersionJava));
    }

    @Test
    public void updateVersionFile_addsCorrectly() throws Exception {
        Version newVersion = new Version(50, 10, 20);
        String versionField = UpdateVersionsTask.toVersionField(newVersion);

        Path versionFile = Path.of("..", UpdateVersionsTask.VERSION_FILE_PATH);
        CompilationUnit unit = LexicalPreservingPrinter.setup(StaticJavaParser.parse(versionFile));
        assertFalse("Test version already exists in the file", findFirstField(unit, versionField).isPresent());

        List<FieldDeclaration> existingFields = unit.findAll(FieldDeclaration.class);

        var result = UpdateVersionsTask.addVersionConstant(unit, newVersion, true);
        assertThat(result.isPresent(), is(true));

        // write out & parse back in again
        StringWriter writer = new StringWriter();
        LexicalPreservingPrinter.print(unit, writer);
        unit = StaticJavaParser.parse(writer.toString());

        // a field has been added
        assertThat(unit.findAll(FieldDeclaration.class), hasSize(existingFields.size() + 1));
        // the field has the right name
        var field = findFirstField(unit, versionField);
        assertThat(field.isPresent(), is(true));
        // the field has the right constant
        assertThat(
            field.get().getVariable(0).getInitializer().get(),
            hasToString(
                String.format("new Version(%d_%02d_%02d_99)", newVersion.getMajor(), newVersion.getMinor(), newVersion.getRevision())
            )
        );
        // and CURRENT has been updated
        var current = findFirstField(unit, "CURRENT");
        assertThat(current.get().getVariable(0).getInitializer().get(), hasToString(versionField));
    }

    @Test
    public void updateVersionFile_removesCorrectly() throws Exception {
        Path versionFile = Path.of("..", UpdateVersionsTask.VERSION_FILE_PATH);
        CompilationUnit unit = LexicalPreservingPrinter.setup(StaticJavaParser.parse(versionFile));

        List<FieldDeclaration> existingFields = unit.findAll(FieldDeclaration.class);

        var staticVersionFields = unit.findAll(
            FieldDeclaration.class,
            f -> f.isStatic() && f.getVariable(0).getTypeAsString().equals("Version")
        );
        // remove the last-but-two static version field (skip CURRENT and the latest version)
        String constant = staticVersionFields.get(staticVersionFields.size() - 3).getVariable(0).getNameAsString();

        Version versionToRemove = UpdateVersionsTask.parseVersionField(constant).orElseThrow(AssertionError::new);
        var result = UpdateVersionsTask.removeVersionConstant(unit, versionToRemove);
        assertThat(result.isPresent(), is(true));

        // write out & parse back in again
        StringWriter writer = new StringWriter();
        LexicalPreservingPrinter.print(unit, writer);
        unit = StaticJavaParser.parse(writer.toString());

        // a field has been removed
        assertThat(unit.findAll(FieldDeclaration.class), hasSize(existingFields.size() - 1));
        // the removed field does not exist
        var field = findFirstField(unit, constant);
        assertThat(field.isPresent(), is(false));
    }

    @Test
    public void addTransportVersion() throws Exception {
        var transportVersions = """
            public class TransportVersions {
                public static final TransportVersion V_1_0_0 = def(1_000_0_00);
                public static final TransportVersion V_1_1_0 = def(1_001_0_00);
                public static final TransportVersion V_1_2_0 = def(1_002_0_00);
                public static final TransportVersion V_1_2_1 = def(1_002_0_01);
                public static final TransportVersion V_1_2_2 = def(1_002_0_02);
                public static final TransportVersion SOME_OTHER_VERSION = def(1_003_0_00);
                public static final TransportVersion YET_ANOTHER_VERSION = def(1_004_0_00);
                public static final TransportVersion MINIMUM_COMPATIBLE = V_1_0_0;
            }
            """;

        var expectedTransportVersions = """
            public class TransportVersions {

                public static final TransportVersion V_1_0_0 = def(1_000_0_00);

                public static final TransportVersion V_1_1_0 = def(1_001_0_00);

                public static final TransportVersion V_1_2_0 = def(1_002_0_00);

                public static final TransportVersion V_1_2_1 = def(1_002_0_01);

                public static final TransportVersion V_1_2_2 = def(1_002_0_02);

                public static final TransportVersion SOME_OTHER_VERSION = def(1_003_0_00);

                public static final TransportVersion YET_ANOTHER_VERSION = def(1_004_0_00);

                public static final TransportVersion NEXT_TRANSPORT_VERSION = def(1_005_0_00);

                public static final TransportVersion MINIMUM_COMPATIBLE = V_1_0_0;
            }
            """;

        var unit = StaticJavaParser.parse(transportVersions);
        var result = UpdateVersionsTask.addTransportVersionConstant(unit, "NEXT_TRANSPORT_VERSION", 1_005_0_00);

        assertThat(result.isPresent(), is(true));
        assertThat(result.get(), hasToString(expectedTransportVersions));
    }

    @Test
    public void addTransportVersionPatch() throws Exception {
        var transportVersions = """
            public class TransportVersions {
                public static final TransportVersion V_1_0_0 = def(1_000_0_00);
                public static final TransportVersion V_1_1_0 = def(1_001_0_00);
                public static final TransportVersion V_1_2_0 = def(1_002_0_00);
                public static final TransportVersion V_1_2_1 = def(1_002_0_01);
                public static final TransportVersion V_1_2_2 = def(1_002_0_02);
                public static final TransportVersion SOME_OTHER_VERSION = def(1_003_0_00);
                public static final TransportVersion YET_ANOTHER_VERSION = def(1_004_0_00);
                public static final TransportVersion MINIMUM_COMPATIBLE = V_1_0_0;
            }
            """;

        var expectedTransportVersions = """
            public class TransportVersions {

                public static final TransportVersion V_1_0_0 = def(1_000_0_00);

                public static final TransportVersion V_1_1_0 = def(1_001_0_00);

                public static final TransportVersion V_1_2_0 = def(1_002_0_00);

                public static final TransportVersion V_1_2_1 = def(1_002_0_01);

                public static final TransportVersion V_1_2_2 = def(1_002_0_02);

                public static final TransportVersion SOME_OTHER_VERSION = def(1_003_0_00);

                public static final TransportVersion PATCH_TRANSPORT_VERSION = def(1_003_0_01);

                public static final TransportVersion YET_ANOTHER_VERSION = def(1_004_0_00);

                public static final TransportVersion MINIMUM_COMPATIBLE = V_1_0_0;
            }
            """;

        var unit = StaticJavaParser.parse(transportVersions);
        var result = UpdateVersionsTask.addTransportVersionConstant(unit, "PATCH_TRANSPORT_VERSION", 1_003_0_01);

        assertThat(result.isPresent(), is(true));
        assertThat(result.get(), hasToString(expectedTransportVersions));
    }

    private static Optional<FieldDeclaration> findFirstField(Node node, String name) {
        return node.findFirst(FieldDeclaration.class, f -> f.getVariable(0).getName().getIdentifier().equals(name));
    }
}
