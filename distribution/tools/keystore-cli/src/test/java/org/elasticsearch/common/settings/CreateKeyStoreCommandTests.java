/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

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
        String password = randomFrom("", "keystorepassword");
        terminal.addSecretInput(password);
        terminal.addSecretInput("notthekeystorepasswordyouarelookingfor");
        UserException e = expectThrows(UserException.class, () -> execute(randomFrom("-p", "--password")));
        assertEquals(e.getMessage(), ExitCodes.DATA_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("Passwords are not equal, exiting"));
    }

    public void testDefaultNotPromptForPassword() throws Exception {
        execute();
        Path configDir = env.configFile();
        assertNotNull(KeyStoreWrapper.load(configDir));
    }

    public void testPosix() throws Exception {
        String password = randomFrom("", "keystorepassword");
        terminal.addSecretInput(password);
        terminal.addSecretInput(password);
        execute();
        Path configDir = env.configFile();
        assertNotNull(KeyStoreWrapper.load(configDir));
    }

    public void testNotPosix() throws Exception {
        String password = randomFrom("", "keystorepassword");
        terminal.addSecretInput(password);
        terminal.addSecretInput(password);
        env = setupEnv(false, fileSystems);
        execute();
        Path configDir = env.configFile();
        assertNotNull(KeyStoreWrapper.load(configDir));
    }

    public void testOverwrite() throws Exception {
        String password = randomFrom("", "keystorepassword");
        Path keystoreFile = KeyStoreWrapper.keystorePath(env.configFile());
        byte[] content = "not a keystore".getBytes(StandardCharsets.UTF_8);
        Files.write(keystoreFile, content);

        terminal.addTextInput(""); // default is no
        execute();
        assertArrayEquals(content, Files.readAllBytes(keystoreFile));

        terminal.addTextInput("n"); // explicit no
        execute();
        assertArrayEquals(content, Files.readAllBytes(keystoreFile));

        terminal.addTextInput("y");
        terminal.addSecretInput(password);
        terminal.addSecretInput(password);
        execute();
        assertNotNull(KeyStoreWrapper.load(env.configFile()));
    }
}
