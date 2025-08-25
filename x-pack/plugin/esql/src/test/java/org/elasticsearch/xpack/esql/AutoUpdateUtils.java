/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.logging.log4j.util.StringBuilders;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class AutoUpdateUtils {
    // TODO: CLI option.
    public static boolean isAutoUpdating = false;

    // TODO: handle more than one update at a time.
    private static void handleAutoUpdate(String expected, String actual, String fileName, int lineNumber) throws IOException {
        var expLineCount = expected.chars().filter(ch -> ch == '\n').count() + 1;

        var outFilePath = Files.createTempFile("autoupdate-", "");
        var writer = new BufferedWriter(new FileWriter(outFilePath.toString()));
        var reader = new BufferedReader(new FileReader(fileName));
        for (int li = 0;; li++) {
            var line = reader.readLine();
            if (line == null) {
                break;
            }
            if (li < lineNumber || li >= lineNumber + expLineCount) {
                writer.write(line + "\n");
            } else if (li == lineNumber) {
                var actualLines = actual.split("\n");
                for (int actualLineId = 0; actualLineId < actualLines.length; actualLineId++) {
                    var sb = new StringBuilder(actualLines[actualLineId]);
                    StringBuilders.escapeJson(sb, 0);
                    writer.write(sb.toString());
                    if (actualLineId + 1 < actualLines.length) {
                        writer.write("\n");
                    }
                }
                writer.write(line.stripLeading() + "\n");
            }
        }
        reader.close();
        writer.close();
        Files.move(outFilePath, Paths.get(fileName), REPLACE_EXISTING);
    }

    public static void assertStr(String expected, String actual) {
        if (expected.equals(actual)) {
            return;
        }
        assert AutoUpdateUtils.isAutoUpdating;

        final var stacktrace = Thread.currentThread().getStackTrace();
        assert stacktrace.length > 2;
        final var callerFrame = stacktrace[2];

        try {
            final String cn = callerFrame.getClassName().replace('.', '/') + ".class";
            final var uri = Class.forName(callerFrame.getClassName()).getClassLoader().getResource(cn);
            assert uri != null;

            // Navigate from .class to .java source file.
            final var fileName = uri.toString()
                .replace(".class", ".java")
                .replace("file:", "")
                .replace("/build/classes/java/test/", "/src/test/java/");

            handleAutoUpdate(expected, actual, fileName, callerFrame.getLineNumber());
        } catch (Exception e) {
            throw new RuntimeException("Encountered an error while auto-updating: " + e.getMessage());
        }
    }
}
