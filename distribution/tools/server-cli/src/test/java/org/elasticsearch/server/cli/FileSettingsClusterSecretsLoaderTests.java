/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;

import static org.elasticsearch.reservedstate.service.FileSettingsService.OPERATOR_DIRECTORY;
import static org.elasticsearch.reservedstate.service.FileSettingsService.SETTINGS_FILE_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class FileSettingsClusterSecretsLoaderTests extends ESTestCase {

    public static final String SETTINGS_JSON = """
        {
             "metadata": {
                 "version": "1",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "ignored": {},
                 "cluster_secrets": {
                     "string_secrets": {
                         "test.secret": "settings.json"
                     }
                 }
             }
        }""";

    private Environment env;

    @Before
    public void setup() {
        env = newEnvironment();
    }

    public void testUsingReservedStateSecrets() throws Exception {
        writeTestFile(OPERATOR_DIRECTORY, SETTINGS_FILE_NAME, SETTINGS_JSON);

        MockTerminal terminal = MockTerminal.create();
        var secretsLoader = new FileSettingsClusterSecretsLoader();
        var loadedSecrets = secretsLoader.load(env, terminal);

        assertThat(loadedSecrets.secrets(), instanceOf(SecureClusterStateSettings.class));
        assertThat(loadedSecrets.secrets().getString("test.secret").toString(), is("settings.json"));
        assertTrue(loadedSecrets.password().isEmpty());

        String output = terminal.getOutput();
        assertThat(output, containsString("Using cluster secrets from file settings"));
        assertThat(output, containsString(SETTINGS_FILE_NAME));
    }

    public void testFallbackToEmpty() {
        var secretsLoader = new FileSettingsClusterSecretsLoader();
        var loadedSecrets = secretsLoader.load(env, MockTerminal.create());
        assertThat(loadedSecrets.secrets(), is(SecureClusterStateSettings.EMPTY));
    }

    public void testUsingEmptyReservedState() throws Exception {
        String settingsJson = """
            {
                 "metadata": {
                     "version": "1",
                     "compatibility": "8.4.0"
                 },
                 "state": {}
            }""";

        writeTestFile(OPERATOR_DIRECTORY, SETTINGS_FILE_NAME, settingsJson);

        var secretsLoader = new FileSettingsClusterSecretsLoader();
        var loadedSecrets = secretsLoader.load(env, MockTerminal.create());
        assertThat(loadedSecrets.secrets(), is(SecureClusterStateSettings.EMPTY));
    }

    public void testBootstrapNotSupported() {
        var secretsLoader = new FileSettingsClusterSecretsLoader();
        assertEquals(
            "Bootstrapping cluster secrets in file settings is not supported",
            expectThrows(IllegalArgumentException.class, () -> secretsLoader.bootstrap(env, new SecureString(new char[0]))).getMessage()
        );
    }

    private void writeTestFile(String directory, String filename, String contents) throws IOException {
        var path = env.configDir().toAbsolutePath().resolve(directory).resolve(filename);
        Files.createDirectories(path.getParent());
        Files.writeString(path, contents);
    }
}
