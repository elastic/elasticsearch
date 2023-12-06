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

public class FreezeVersionTaskTests {

    @Test
    public void updateVersion_updatesCorrectly() throws Exception {
        Version newVersion = new Version(50, 1, 2);
        String versionField = String.format("V_%d_%d_%d", newVersion.getMajor(), newVersion.getMinor(), newVersion.getRevision());

        Path versionFile = Path.of("..", FreezeVersionTask.VERSION_PATH);
        CompilationUnit unit = LexicalPreservingPrinter.setup(StaticJavaParser.parse(versionFile));
        assertThat("Test version already exists in the file", findFirstField(unit, versionField).isEmpty(), is(true));

        List<FieldDeclaration> existingFields = unit.findAll(FieldDeclaration.class);

        // do the update
        FreezeVersionTask.updateVersionJava(unit, newVersion);

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
