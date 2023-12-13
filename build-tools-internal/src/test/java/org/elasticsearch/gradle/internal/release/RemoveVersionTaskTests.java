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
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

public class RemoveVersionTaskTests {
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

        var newUnit = RemoveVersionTask.removeVersionConstant(unit, Version.fromString("8.10.2"));
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
            () -> RemoveVersionTask.removeVersionConstant(unit, Version.fromString("8.11.0"))
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

        RemoveVersionTask.removeVersionConstant(unit, Version.fromString("8.10.1"));

        assertThat(unit, hasToString(updatedVersionJava));
    }

    private static final Pattern VERSION_FIELD = Pattern.compile("V_(\\d+)_(\\d+)_(\\d+)(?:_(\\w+))?");

    @Test
    public void updateVersionFile_updatesCorrectly() throws Exception {
        Path versionFile = Path.of("..", AddVersionTask.VERSION_FILE_PATH);
        CompilationUnit unit = LexicalPreservingPrinter.setup(StaticJavaParser.parse(versionFile));

        List<FieldDeclaration> existingFields = unit.findAll(FieldDeclaration.class);

        var staticVersionFields = unit.findAll(
            FieldDeclaration.class,
            f -> f.isStatic() && f.getVariable(0).getTypeAsString().equals("Version")
        );
        // remove the last-but-two static version field (skip CURRENT and the latest version)
        String constant = staticVersionFields.get(staticVersionFields.size() - 3).getVariable(0).getNameAsString();
        var version = VERSION_FIELD.matcher(constant);
        assertThat(version.find(), is(true));

        Version versionToRemove = new Version(
            Integer.parseInt(version.group(1)),
            Integer.parseInt(version.group(2)),
            Integer.parseInt(version.group(3)),
            version.group(4)
        );
        var result = RemoveVersionTask.removeVersionConstant(unit, versionToRemove);
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
