/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class UpdateVersionsTaskTests {

    @Test
    public void addVersion_versionExists() {
        final String versionJava = """
            public class Version {
                public static final Version V_7_16_0 = new Version(7_16_00_99, org.apache.lucene.util.Version.LUCENE_8_10_1);
                public static final Version V_7_16_1 = new Version(7_16_01_99, org.apache.lucene.util.Version.LUCENE_8_10_1);
                public static final Version V_7_17_0 = new Version(7_17_00_99, org.apache.lucene.util.Version.LUCENE_8_11_1);
                public static final Version CURRENT = V_7_17_0;
            }""";

        CompilationUnit unit = StaticJavaParser.parse(versionJava);

        var newUnit = UpdateVersionsTask.addVersionConstant(unit, Version.fromString("7.17.0"), false);
        assertThat(newUnit.isPresent(), is(false));
    }

    @Test
    public void addVersion_oldVersion() {
        final String versionJava = """
            public class Version {
                public static final Version V_7_16_0 = new Version(7_16_00_99, org.apache.lucene.util.Version.LUCENE_8_10_1);
                public static final Version V_7_16_1 = new Version(7_16_01_99, org.apache.lucene.util.Version.LUCENE_8_10_1);
                public static final Version V_7_17_0 = new Version(7_17_00_99, org.apache.lucene.util.Version.LUCENE_8_11_1);
                public static final Version CURRENT = V_7_17_0;
            }""";
        final String updatedVersionJava = """
            public class Version {

                public static final Version V_7_16_0 = new Version(7_16_00_99, org.apache.lucene.util.Version.LUCENE_8_10_1);

                public static final Version V_7_16_1 = new Version(7_16_01_99, org.apache.lucene.util.Version.LUCENE_8_10_1);

                public static final Version V_7_16_2 = new Version(7_16_02_99, org.apache.lucene.util.Version.LUCENE_8_10_1);

                public static final Version V_7_17_0 = new Version(7_17_00_99, org.apache.lucene.util.Version.LUCENE_8_11_1);

                public static final Version CURRENT = V_7_17_0;
            }
            """;

        CompilationUnit unit = StaticJavaParser.parse(versionJava);

        UpdateVersionsTask.addVersionConstant(unit, Version.fromString("7.16.2"), false);

        assertThat(unit, hasToString(updatedVersionJava));
    }

    @Test
    public void addVersion_newVersion_current() {
        final String versionJava = """
            public class Version {
                public static final Version V_7_16_0 = new Version(7_16_00_99, org.apache.lucene.util.Version.LUCENE_8_10_1);
                public static final Version V_7_16_1 = new Version(7_16_01_99, org.apache.lucene.util.Version.LUCENE_8_10_1);
                public static final Version V_7_17_0 = new Version(7_17_00_99, org.apache.lucene.util.Version.LUCENE_8_11_1);
                public static final Version CURRENT = V_7_17_0;
            }""";
        final String updatedVersionJava = """
            public class Version {

                public static final Version V_7_16_0 = new Version(7_16_00_99, org.apache.lucene.util.Version.LUCENE_8_10_1);

                public static final Version V_7_16_1 = new Version(7_16_01_99, org.apache.lucene.util.Version.LUCENE_8_10_1);

                public static final Version V_7_17_0 = new Version(7_17_00_99, org.apache.lucene.util.Version.LUCENE_8_11_1);

                public static final Version V_7_17_1 = new Version(7_17_01_99, org.apache.lucene.util.Version.LUCENE_8_11_1);

                public static final Version CURRENT = V_7_17_1;
            }
            """;

        CompilationUnit unit = StaticJavaParser.parse(versionJava);

        UpdateVersionsTask.addVersionConstant(unit, Version.fromString("7.17.1"), true);

        assertThat(unit, hasToString(updatedVersionJava));
    }

    @Test
    public void removeVersion_versionDoesntExist() {
        final String versionJava = """
            public class Version {
                public static final Version V_7_16_0 = new Version(7_16_00_99, org.apache.lucene.util.Version.LUCENE_8_10_1);
                public static final Version V_7_16_1 = new Version(7_16_01_99, org.apache.lucene.util.Version.LUCENE_8_10_1);
                public static final Version V_7_17_0 = new Version(7_17_00_99, org.apache.lucene.util.Version.LUCENE_8_11_1);
                public static final Version CURRENT = V_7_17_0;
            }""";

        CompilationUnit unit = StaticJavaParser.parse(versionJava);

        var newUnit = UpdateVersionsTask.removeVersionConstant(unit, Version.fromString("7.16.2"));
        assertThat(newUnit.isPresent(), is(false));
    }

    @Test
    public void removeVersion_versionIsCurrent() {
        final String versionJava = """
            public class Version {
                public static final Version V_7_16_0 = new Version(7_16_00_99, org.apache.lucene.util.Version.LUCENE_8_10_1);
                public static final Version V_7_16_1 = new Version(7_16_01_99, org.apache.lucene.util.Version.LUCENE_8_10_1);
                public static final Version V_7_17_0 = new Version(7_17_00_99, org.apache.lucene.util.Version.LUCENE_8_11_1);
                public static final Version CURRENT = V_7_17_0;
            }""";

        CompilationUnit unit = StaticJavaParser.parse(versionJava);

        var ex = assertThrows(
            IllegalArgumentException.class,
            () -> UpdateVersionsTask.removeVersionConstant(unit, Version.fromString("7.17.0"))
        );
        assertThat(ex.getMessage(), equalTo("Cannot remove version [7.17.0], it is referenced by CURRENT"));
    }

    @Test
    public void removeVersion() {
        final String versionJava = """
            public class Version {
                public static final Version V_7_16_0 = new Version(7_16_00_99, org.apache.lucene.util.Version.LUCENE_8_10_1);
                public static final Version V_7_16_1 = new Version(7_16_01_99, org.apache.lucene.util.Version.LUCENE_8_10_1);
                public static final Version V_7_17_0 = new Version(7_17_00_99, org.apache.lucene.util.Version.LUCENE_8_11_1);
                public static final Version CURRENT = V_7_17_0;
            }""";
        final String updatedVersionJava = """
            public class Version {

                public static final Version V_7_16_0 = new Version(7_16_00_99, org.apache.lucene.util.Version.LUCENE_8_10_1);

                public static final Version V_7_17_0 = new Version(7_17_00_99, org.apache.lucene.util.Version.LUCENE_8_11_1);

                public static final Version CURRENT = V_7_17_0;
            }
            """;

        CompilationUnit unit = StaticJavaParser.parse(versionJava);

        UpdateVersionsTask.removeVersionConstant(unit, Version.fromString("7.16.1"));

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
                matchesRegex(
                    String.format(
                        "new Version\\(%d_%02d_%02d_99, (org.apache.lucene.util.Version.)?LUCENE_[0-9_]+\\)",
                        newVersion.getMajor(),
                        newVersion.getMinor(),
                        newVersion.getRevision()
                    )
                )
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

    private static Optional<FieldDeclaration> findFirstField(Node node, String name) {
        return node.findFirst(FieldDeclaration.class, f -> f.getVariable(0).getName().getIdentifier().equals(name));
    }
}
