/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.useragent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class UserAgentParserRegistry {

    private static final Logger logger = LogManager.getLogger(UserAgentParserRegistry.class);

    static final String DEFAULT_PARSER_NAME = "_default_";

    private final Map<String, UserAgentParser> registry;

    UserAgentParserRegistry(UserAgentCache cache, Path... regexFileDirectories) {
        registry = createParsersMap(cache, regexFileDirectories);
    }

    static Map<String, UserAgentParser> createParsersMap(UserAgentCache cache, Path... regexFileDirectories) {
        Map<String, UserAgentParser> userAgentParsers = new HashMap<>();

        UserAgentParser defaultParser = new UserAgentParser(
            DEFAULT_PARSER_NAME,
            UserAgentPlugin.class.getResourceAsStream("/regexes.yml"),
            UserAgentPlugin.class.getResourceAsStream("/device_type_regexes.yml"),
            cache
        );
        userAgentParsers.put(DEFAULT_PARSER_NAME, defaultParser);

        for (Path regexFileDirectory : regexFileDirectories) {
            readAndAppendUParsers(regexFileDirectory, cache, userAgentParsers);
        }
        return Map.copyOf(userAgentParsers);
    }

    private static void readAndAppendUParsers(
        Path userAgentConfigDirectory,
        UserAgentCache cache,
        Map<String, UserAgentParser> userAgentParsers
    ) {
        if (Files.exists(userAgentConfigDirectory) && Files.isDirectory(userAgentConfigDirectory)) {
            PathMatcher pathMatcher = userAgentConfigDirectory.getFileSystem().getPathMatcher("glob:**.yml");
            try (
                Stream<Path> regexFiles = Files.find(
                    userAgentConfigDirectory,
                    1,
                    (path, attr) -> attr.isRegularFile() && pathMatcher.matches(path)
                )
            ) {
                Iterable<Path> iterable = regexFiles::iterator;
                for (Path path : iterable) {
                    String parserName = path.getFileName().toString();
                    try (
                        InputStream regexStream = Files.newInputStream(path, StandardOpenOption.READ);
                        InputStream deviceTypeRegexStream = UserAgentPlugin.class.getResourceAsStream("/device_type_regexes.yml")
                    ) {
                        userAgentParsers.put(parserName, new UserAgentParser(parserName, regexStream, deviceTypeRegexStream, cache));
                    }
                }
            } catch (IOException e) {
                logger.error("Error reading custom user agent regex files, falling back to builtin regex files", e);
            }
        }
    }

    UserAgentParser getParser(String parserName) {
        return registry.get(parserName);
    }
}
