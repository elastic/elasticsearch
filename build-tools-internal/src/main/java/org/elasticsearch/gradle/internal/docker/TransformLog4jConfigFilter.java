/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.docker;

import org.apache.commons.io.IOUtils;

import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class TransformLog4jConfigFilter extends FilterReader {
    public TransformLog4jConfigFilter(Reader in) throws IOException {
        super(new StringReader(transform(in)));
    }

    private static String transform(Reader reader) throws IOException {
        final List<String> inputLines = IOUtils.readLines(reader);
        final List<String> outputLines = skipBlanks(transformConfig(inputLines));
        return String.join("\n", outputLines);
    }

    /** Squeeze multiple empty lines into a single line. */
    static List<String> skipBlanks(List<String> lines) {
        boolean skipNextEmpty = false;

        final List<String> output = new ArrayList<>(lines.size());

        for (final String line : lines) {
            if (line.isEmpty()) {
                if (skipNextEmpty) {
                    continue;
                } else {
                    skipNextEmpty = true;
                }
            } else {
                skipNextEmpty = false;
            }

            output.add(line);
        }

        return output;
    }

    static List<String> transformConfig(List<String> lines) {
        final List<String> output = new ArrayList<>(lines.size());

        // This flag provides a way to handle properties whose values are split
        // over multiple lines and we need to omit those properties.
        boolean skipNext = false;

        for (String line : lines) {
            if (skipNext) {
                if (line.endsWith("\\") == false) {
                    skipNext = false;
                }
                continue;
            }

            // Skip lines with this comment - we remove the relevant config
            if (line.contains("old style pattern")) {
                skipNext = line.endsWith("\\");
                continue;
            }

            if (line.startsWith("appender.")) {
                String[] parts = line.split("\\s*=\\s*");
                String key = parts[0];
                String[] keyParts = key.split("\\.");
                String value = parts[1];

                // We don't need to explicitly define a console appender because the
                // "rolling" appender will become a console appender. We also don't
                // carry over "*_old" appenders
                if (keyParts[1].equals("console") || keyParts[1].endsWith("_old")) {
                    skipNext = line.endsWith("\\");
                    continue;
                }

                switch (keyParts[2]) {
                    case "type":
                        if (value.equals("RollingFile")) {
                            value = "Console";
                        }
                        line = key + " = " + value;
                        break;

                    case "fileName":
                    case "filePattern":
                    case "policies":
                    case "strategy":
                        // No longer applicable. Omit it.
                        skipNext = line.endsWith("\\");
                        continue;

                    default:
                        break;
                }
            } else if (line.startsWith("rootLogger.appenderRef")) {
                String[] parts = line.split("\\s*=\\s*");

                // The root logger only needs this appender
                if (parts[1].equals("rolling") == false) {
                    skipNext = line.endsWith("\\");
                    continue;
                }
            } else if (line.startsWith("logger.")) {
                String[] parts = line.split("\\s*=\\s*");
                String key = parts[0];
                String[] keyParts = key.split("\\.");

                if (keyParts[2].equals("appenderRef") && keyParts[3].endsWith("_old")) {
                    skipNext = line.endsWith("\\");
                    continue;
                }
            }

            output.add(line);
        }

        return output;
    }
}
