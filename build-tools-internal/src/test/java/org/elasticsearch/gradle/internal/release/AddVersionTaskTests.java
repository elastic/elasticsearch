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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;

public class AddVersionTaskTests {

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

        var newUnit = AddVersionTask.addVersionConstant(unit, Version.fromString("8.10.1"), false);
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

        AddVersionTask.addVersionConstant(unit, Version.fromString("8.10.2"), false);

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

        AddVersionTask.addVersionConstant(unit, Version.fromString("8.11.1"), true);

        assertThat(unit, hasToString(updatedVersionJava));
    }

    @Test
    public void updateVersionFile_updatesCorrectly() throws Exception {
        Version newVersion = new Version(50, 10, 20);
        String versionField = AbstractVersionTask.toVersionField(newVersion);

        Path versionFile = Path.of("..", AddVersionTask.VERSION_FILE_PATH);
        CompilationUnit unit = LexicalPreservingPrinter.setup(StaticJavaParser.parse(versionFile));
        assertFalse("Test version already exists in the file", findFirstField(unit, versionField).isPresent());

        List<FieldDeclaration> existingFields = unit.findAll(FieldDeclaration.class);

        var result = AddVersionTask.addVersionConstant(unit, newVersion, true);
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

    private static Optional<FieldDeclaration> findFirstField(Node node, String name) {
        return node.findFirst(FieldDeclaration.class, f -> f.getVariable(0).getName().getIdentifier().equals(name));
    }
}
