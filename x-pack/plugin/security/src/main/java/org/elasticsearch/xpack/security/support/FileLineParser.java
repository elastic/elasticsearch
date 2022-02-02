/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.common.CheckedBiConsumer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class FileLineParser {
    public static void parse(Path path, CheckedBiConsumer<Integer, String, IOException> lineParser) throws IOException {
        final List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);

        int lineNumber = 0;
        for (String line : lines) {
            lineNumber++;
            if (line.startsWith("#") || Strings.isBlank(line)) { // comment or blank
                continue;
            }

            lineParser.accept(lineNumber, line);
        }
    }
}
