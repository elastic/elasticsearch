/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.useragent;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.useragent.api.UserAgentParser;
import org.elasticsearch.useragent.api.UserAgentParserRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UserAgentParserRegistryImplTests extends ESTestCase {

    public void testDefaultParserAlwaysAvailable() {
        UserAgentCache cache = new UserAgentCache(100);
        Map<String, UserAgentParserImpl> parsers = UserAgentParserRegistryImpl.createParsersMap(cache);
        assertThat(parsers.get(UserAgentParserRegistry.DEFAULT_PARSER_NAME), notNullValue());
    }

    public void testCustomRegexFileRegisteredWithYmlExtension() throws IOException {
        Path configDir = createTempDir();
        copyBuiltinRegexesTo(configDir, "custom-regexes.yml");

        UserAgentCache cache = new UserAgentCache(100);
        Map<String, UserAgentParserImpl> parsers = UserAgentParserRegistryImpl.createParsersMap(cache, configDir);

        // The registry key includes the .yml extension
        assertThat(parsers.get("custom-regexes.yml"), notNullValue());

        // Without extension does NOT match
        assertThat(parsers.get("custom-regexes"), nullValue());
    }

    public void testCustomRegexFileProducesValidParser() throws IOException {
        Path configDir = createTempDir();
        copyBuiltinRegexesTo(configDir, "my-regexes.yml");

        UserAgentCache cache = new UserAgentCache(100);
        Map<String, UserAgentParserImpl> parsers = UserAgentParserRegistryImpl.createParsersMap(cache, configDir);

        UserAgentParser parser = parsers.get("my-regexes.yml");
        assertThat(parser, notNullValue());

        // Verify the custom parser can actually parse a user-agent string
        var details = parser.parseUserAgentInfo(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.149 Safari/537.36",
            false
        );
        assertThat(details, notNullValue());
        assertEquals("Chrome", details.name());
    }

    public void testNonYmlFilesIgnored() throws IOException {
        Path configDir = createTempDir();
        Files.writeString(configDir.resolve("not-a-regex.txt"), "this is not yaml");

        UserAgentCache cache = new UserAgentCache(100);
        Map<String, UserAgentParserImpl> parsers = UserAgentParserRegistryImpl.createParsersMap(cache, configDir);

        // Only the default parser should exist
        assertEquals(1, parsers.size());
        assertThat(parsers.get(UserAgentParserRegistry.DEFAULT_PARSER_NAME), notNullValue());
    }

    public void testMultipleConfigDirectories() throws IOException {
        Path configDir1 = createTempDir();
        Path configDir2 = createTempDir();
        copyBuiltinRegexesTo(configDir1, "regexes-a.yml");
        copyBuiltinRegexesTo(configDir2, "regexes-b.yml");

        UserAgentCache cache = new UserAgentCache(100);
        Map<String, UserAgentParserImpl> parsers = UserAgentParserRegistryImpl.createParsersMap(cache, configDir1, configDir2);

        assertThat(parsers.get(UserAgentParserRegistry.DEFAULT_PARSER_NAME), notNullValue());
        assertThat(parsers.get("regexes-a.yml"), notNullValue());
        assertThat(parsers.get("regexes-b.yml"), notNullValue());
    }

    public void testNonExistentDirectoryIgnored() {
        Path nonExistent = createTempDir().resolve("does-not-exist");
        UserAgentCache cache = new UserAgentCache(100);
        Map<String, UserAgentParserImpl> parsers = UserAgentParserRegistryImpl.createParsersMap(cache, nonExistent);

        // Only the default parser should exist
        assertEquals(1, parsers.size());
        assertThat(parsers.get(UserAgentParserRegistry.DEFAULT_PARSER_NAME), notNullValue());
    }

    /**
     * Copies the builtin regexes.yml into the given directory with the specified filename.
     */
    private static void copyBuiltinRegexesTo(Path directory, String filename) throws IOException {
        try (InputStream builtinRegexes = UserAgentPlugin.class.getResourceAsStream("/regexes.yml")) {
            assertNotNull("builtin regexes.yml not found", builtinRegexes);
            Files.copy(builtinRegexes, directory.resolve(filename));
        }
    }
}
