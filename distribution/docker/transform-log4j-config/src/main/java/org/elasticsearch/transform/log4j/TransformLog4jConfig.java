/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        List<String> lines = getConfigFile(args);

        final List<String> output = transformConfig(lines);

        output.forEach(System.out::println);
    }

    private static List<String> getConfigFile(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("ERROR: Must supply a single argument, the file to process");
            System.exit(1);
        }

        Path configPath = Path.of(args[0]);

        if (Files.exists(configPath) == false) {
            System.err.println("ERROR: [" + configPath + "] does not exist");
            System.exit(1);
        }

        if (Files.isReadable(configPath) == false) {
            System.err.println("ERROR: [" + configPath + "] exists but is not readable");
            System.exit(1);
        }

        return Files.readAllLines(configPath);
    }

    public static List<String> transformConfig(List<String> lines) {
        final List<String> output = new ArrayList<>(lines.size());

        // This flag provides a way to handle properties whose values are split
        // over multiple lines and we need to omit those properties.
        boolean skipNext = false;

        for (final String line : lines) {
            if (skipNext) {
                if (line.endsWith("\\") == false) {
                    skipNext = false;
                }
                continue;
            }
            if (line.startsWith("appender.")) {
                String[] parts = line.split("\\s*=\\s*");
                String key = parts[0];
                String[] keyParts = key.split("\\.");
                String value = parts[1];

                // We don't need to explicitly define a console appender because the
                // "rolling" appender will become a console appender. We also don't
                // carry over "rolling_old"
                if (keyParts[1].equals("console") || keyParts[1].equals("rolling_old")) {
                    continue;
                }

                switch (keyParts[2]) {
                    case "type":
                        if (value.equals("RollingFile")) {
                            value = "Console";
                        }
                        output.add(key + " = " + value);
                        break;

                    case "fileName":
                    case "filePattern":
                    case "policies":
                    case "strategy":
                        // No longer applicable. Omit it.
                        skipNext = line.endsWith("\\");
                        break;

                    default:
                        output.add(line);
                        break;
                }
            } else if (line.startsWith("rootLogger.appenderRef")) {
                String[] parts = line.split("\\s*=\\s*");

                // The root logger only needs this appender
                if (parts[1].equals("rolling")) {
                    output.add(line);
                }
            } else {
                output.add(line);
            }
        }

        return output;
    }
}
