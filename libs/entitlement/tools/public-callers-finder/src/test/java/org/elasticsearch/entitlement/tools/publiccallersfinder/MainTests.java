/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.publiccallersfinder;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.hasItem;

public class MainTests extends ESTestCase {

    record OutputRow(
        String moduleName,
        String source,
        String line,
        String className,
        String methodName,
        String methodDescriptor,
        String access,
        String originalModule,
        String originalClassName,
        String originalMethodName,
        String originalAccess
    ) {
        OutputRow(String[] row) {
            this(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]);
        }
    }

    public void testTransitiveFindsDeepCallChain() throws Exception {
        URI uri = getClass().getResource("public-callers-finder-test-input.tsv").toURI();
        List<OutputRow> rows = parseOutput(runWithTransitive(PathUtils.get(uri).toAbsolutePath()));

        var expectedRow = new OutputRow(
            "java.base",
            "System.java",
            "1540",
            "java/lang/System",
            "exit",
            "(I)V",
            "PUBLIC_CLASS:PUBLIC_METHOD",
            "java.base",
            "java/lang/Shutdown",
            "halt",
            "PUBLIC_CLASS:PUBLIC_METHOD"
        );
        assertThat(rows, hasItem(expectedRow));
    }

    private List<OutputRow> parseOutput(String output) {
        return output.lines().filter(line -> line.isEmpty() == false).map(line -> line.split("\t")).map(OutputRow::new).toList();
    }

    private String runWithTransitive(Path csvFilePath) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(captured, false, StandardCharsets.UTF_8);
        Main.run(csvFilePath, true, false, false, out);
        return captured.toString(StandardCharsets.UTF_8);
    }
}
