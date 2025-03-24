/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.useragent;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class IngestUserAgentPlugin extends Plugin implements IngestPlugin {

    private final Setting<Long> CACHE_SIZE_SETTING = Setting.longSetting(
        "ingest.user_agent.cache_size",
        1000,
        0,
        Setting.Property.NodeScope
    );

    static final String DEFAULT_PARSER_NAME = "_default_";

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        Path userAgentConfigDirectory = parameters.env.configDir().resolve("ingest-user-agent");

        if (Files.exists(userAgentConfigDirectory) == false && Files.isDirectory(userAgentConfigDirectory)) {
            throw new IllegalStateException(
                "the user agent directory [" + userAgentConfigDirectory + "] containing the regex file doesn't exist"
            );
        }

        long cacheSize = CACHE_SIZE_SETTING.get(parameters.env.settings());
        Map<String, UserAgentParser> userAgentParsers;
        try {
            userAgentParsers = createUserAgentParsers(userAgentConfigDirectory, new UserAgentCache(cacheSize));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Map.of(UserAgentProcessor.TYPE, new UserAgentProcessor.Factory(userAgentParsers));
    }

    static Map<String, UserAgentParser> createUserAgentParsers(Path userAgentConfigDirectory, UserAgentCache cache) throws IOException {
        Map<String, UserAgentParser> userAgentParsers = new HashMap<>();

        UserAgentParser defaultParser = new UserAgentParser(
            DEFAULT_PARSER_NAME,
            IngestUserAgentPlugin.class.getResourceAsStream("/regexes.yml"),
            IngestUserAgentPlugin.class.getResourceAsStream("/device_type_regexes.yml"),
            cache
        );
        userAgentParsers.put(DEFAULT_PARSER_NAME, defaultParser);

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
                        InputStream deviceTypeRegexStream = IngestUserAgentPlugin.class.getResourceAsStream("/device_type_regexes.yml")
                    ) {
                        userAgentParsers.put(parserName, new UserAgentParser(parserName, regexStream, deviceTypeRegexStream, cache));
                    }
                }
            }
        }

        return Map.copyOf(userAgentParsers);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(CACHE_SIZE_SETTING);
    }

    @Override
    public Map<String, UnaryOperator<Metadata.ProjectCustom>> getProjectCustomMetadataUpgraders() {
        return Map.of(
            IngestMetadata.TYPE,
            ingestMetadata -> ((IngestMetadata) ingestMetadata).maybeUpgradeProcessors(
                UserAgentProcessor.TYPE,
                UserAgentProcessor::maybeUpgradeConfig
            )
        );
    }
}
