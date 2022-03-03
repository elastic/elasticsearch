/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.keystore;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.env.Environment;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class CreateKeyStoreCommandTests extends KeyStoreCommandTestCase {

    @Override
    protected Command newCommand() {
        return new CreateKeyStoreCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return env;
            }
        };
    }

    public void testNotMatchingPasswords() throws Exception {
        String password = getPossibleKeystorePassword();
        terminal.addSecretInput(password);
        terminal.addSecretInput("notthekeystorepasswordyouarelookingfor");
        UserException e = expectThrows(UserException.class, () -> execute(randomFrom("-p", "--password")));
        assertEquals(e.getMessage(), ExitCodes.DATA_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("Passwords are not equal, exiting"));
    }

    public void testDefaultNotPromptForPassword() throws Exception {
        assumeFalse("Cannot open unprotected keystore on FIPS JVM", inFipsJvm());
        execute();
        Path configDir = env.configFile();
        assertNotNull(KeyStoreWrapper.load(configDir));
    }

    public void testPosix() throws Exception {
        final String password = getPossibleKeystorePassword();
        // Sometimes (rarely) test with explicit empty password
        final boolean withPassword = password.length() > 0 || rarely();
        if (withPassword) {
            terminal.addSecretInput(password);
            terminal.addSecretInput(password);
            execute(randomFrom("-p", "--password"));
        } else {
            execute();
        }
        Path configDir = env.configFile();
        assertNotNull(KeyStoreWrapper.load(configDir));
    }

    public void testNotPosix() throws Exception {
        env = setupEnv(false, fileSystems);
        final String password = getPossibleKeystorePassword();
        // Sometimes (rarely) test with explicit empty password
        final boolean withPassword = password.length() > 0 || rarely();
        if (withPassword) {
            terminal.addSecretInput(password);
            terminal.addSecretInput(password);
            execute(randomFrom("-p", "--password"));
        } else {
            execute();
        }
        Path configDir = env.configFile();
        assertNotNull(KeyStoreWrapper.load(configDir));
    }

    public void testOverwrite() throws Exception {
        String password = getPossibleKeystorePassword();
        Path keystoreFile = KeyStoreWrapper.keystorePath(env.configFile());
        byte[] content = "not a keystore".getBytes(StandardCharsets.UTF_8);
        Files.write(keystoreFile, content);

        terminal.addTextInput(""); // default is no (don't overwrite)
        execute();
        assertArrayEquals(content, Files.readAllBytes(keystoreFile));

        terminal.addTextInput("n"); // explicit no (don't overwrite)
        execute();
        assertArrayEquals(content, Files.readAllBytes(keystoreFile));

        terminal.addTextInput("y"); // overwrite
        // Sometimes (rarely) test with explicit empty password
        final boolean withPassword = password.length() > 0 || rarely();
        if (withPassword) {
            terminal.addSecretInput(password);
            terminal.addSecretInput(password);
            execute(randomFrom("-p", "--password"));
        } else {
            execute();
        }
        assertNotNull(KeyStoreWrapper.load(env.configFile()));
    }
}
