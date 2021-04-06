/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transform.log4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * This class takes in a log4j configuration file, and transform it into a config that
 * writes everything to the console. This is useful when running Elasticsearch in a Docker
 * container, where the Docker convention is to log to stdout / stderr and let the
 * orchestration layer direct the output.
 */
public class TransformLog4jConfig {

    public static void main(String[] args) throws IOException {
        validateArguments(args);

        final Path inputPath = Path.of(args[0]);
        final Path outputPath = Path.of(args[1]);

        final List<String> inputLines = Files.readAllLines(inputPath);
        final List<String> outputLines = skipBlanks(transformConfig(inputLines));

        Files.write(outputPath, outputLines);
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

    private static void validateArguments(String[] args) {
        if (args.length != 2) {
            System.err.println("ERROR: Must supply two arguments, the input file and the output file");
            System.exit(1);
        }

        Path configPath = Path.of(args[0]);

        if (Files.exists(configPath) == false) {
            System.err.println("ERROR: Input path [" + configPath + "] does not exist");
            System.exit(1);
        }

        if (Files.isReadable(configPath) == false) {
            System.err.println("ERROR: Input path [" + configPath + "] exists but is not readable");
            System.exit(1);
        }

        Path outputPath = Path.of(args[1]);

        if (Files.isWritable(outputPath.getParent()) == false) {
            System.err.println("ERROR: Output directory [" + outputPath.getParent() + "] is not writable");
            System.exit(1);
        }
    }
}
