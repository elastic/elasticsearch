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

import java.io.ByteArrayInputStream;
import java.io.CharArrayWriter;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class AddStringKeyStoreCommandTests extends KeyStoreCommandTestCase {
    InputStream input;

    @Override
    protected Command newCommand() {
        return new AddStringKeyStoreCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return env;
            }

            @Override
            InputStream getStdin() {
                return input;
            }
        };
    }

    public void testInvalidPassphrease() throws Exception {
        String passphrase = "keystorepassphrase";
        createKeystore(passphrase, "foo", "bar");
        terminal.addSecretInput("thewrongpassphrase");
        UserException e = expectThrows(UserException.class, () -> execute("foo2"));
        assertEquals(e.getMessage(), ExitCodes.DATA_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("Please make sure the passphrase was correct"));

    }

    public void testMissingPromptCreate() throws Exception {
        String passphrase = "keystorepassphrase";
        terminal.addTextInput("y");
        terminal.addSecretInput(passphrase);
        terminal.addSecretInput(passphrase);
        terminal.addSecretInput("bar");
        execute("foo");
        assertSecureString("foo", "bar", passphrase);
    }

    public void testMissingPromptCreateNotMatchingPasswords() throws Exception {
        String passphrase = "keystorepassphrase";
        terminal.addTextInput("y");
        terminal.addSecretInput(passphrase);
        terminal.addSecretInput("anotherpassphrase");
        terminal.addSecretInput("bar");
        UserException e = expectThrows(UserException.class, () -> execute("foo"));
        assertEquals(e.getMessage(), ExitCodes.DATA_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("Passphrases are not equal, exiting"));
    }

    public void testMissingForceCreate() throws Exception {
        String passphrase = "keystorepassphrase";
        terminal.addSecretInput(passphrase);
        terminal.addSecretInput(passphrase);
        terminal.addSecretInput("bar");
        execute("-f", "foo");
        assertSecureString("foo", "bar", passphrase);
    }

    public void testMissingNoCreate() throws Exception {
        terminal.addTextInput("n"); // explicit no
        execute("foo");
        assertNull(KeyStoreWrapper.load(env.configFile()));
    }

    public void testOverwritePromptDefault() throws Exception {
        String passphrase = "keystorepassphrase";
        createKeystore(passphrase, "foo", "bar");
        terminal.addSecretInput(passphrase);
        terminal.addTextInput("");
        execute("foo");
        assertSecureString("foo", "bar", passphrase);
    }

    public void testOverwritePromptExplicitNo() throws Exception {
        String passphrase = "keystorepassphrase";
        createKeystore(passphrase, "foo", "bar");
        terminal.addSecretInput(passphrase);
        terminal.addTextInput("n"); // explicit no
        execute("foo");
        assertSecureString("foo", "bar", passphrase);
    }

    public void testOverwritePromptExplicitYes() throws Exception {
        String passphrase = "keystorepassphrase";
        createKeystore(passphrase, "foo", "bar");
        terminal.addTextInput("y");
        terminal.addSecretInput(passphrase);
        terminal.addSecretInput("newvalue");
        execute("foo");
        assertSecureString("foo", "newvalue", passphrase);
    }

    public void testOverwriteForceShort() throws Exception {
        String passphrase = "keystorepassphrase";
        createKeystore(passphrase, "foo", "bar");
        terminal.addSecretInput(passphrase);
        terminal.addSecretInput("newvalue");
        execute("-f", "foo"); // force
        assertSecureString("foo", "newvalue", passphrase);
    }

    public void testOverwriteForceLong() throws Exception {
        String passphrase = "keystorepassphrase";
        createKeystore(passphrase, "foo", "bar");
        terminal.addSecretInput(passphrase);
        terminal.addSecretInput("and yet another secret value");
        execute("--force", "foo"); // force
        assertSecureString("foo", "and yet another secret value", passphrase);
    }

    public void testForceNonExistent() throws Exception {
        String passphrase = "keystorepassphrase";
        createKeystore(passphrase);
        terminal.addSecretInput(passphrase);
        terminal.addSecretInput("value");
        execute("--force", "foo"); // force
        assertSecureString("foo", "value", passphrase);
    }

    public void testPromptForValue() throws Exception {
        String passphrase = "keystorepassphrase";
        KeyStoreWrapper.create().save(env.configFile(), passphrase.toCharArray());
        terminal.addSecretInput(passphrase);
        terminal.addSecretInput("secret value");
        execute("foo");
        assertSecureString("foo", "secret value", passphrase);
    }

    public void testStdinShort() throws Exception {
        String passphrase = "keystorepassphrase";
        KeyStoreWrapper.create().save(env.configFile(), passphrase.toCharArray());
        terminal.addSecretInput(passphrase);
        setInput("secret value 1");
        execute("-x", "foo");
        assertSecureString("foo", "secret value 1", passphrase);
    }

    public void testStdinLong() throws Exception {
        String passphrase = "keystorepassphrase";
        KeyStoreWrapper.create().save(env.configFile(), passphrase.toCharArray());
        terminal.addSecretInput(passphrase);
        setInput("secret value 2");
        execute("--stdin", "foo");
        assertSecureString("foo", "secret value 2", passphrase);
    }

    public void testMissingSettingName() throws Exception {
        String passphrase = "keystorepassphrase";
        createKeystore(passphrase);
        terminal.addSecretInput(passphrase);
        terminal.addSecretInput(passphrase);
        terminal.addTextInput("");
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("The setting name can not be null"));
    }

    public void testAddToUnprotectedKeystore() throws Exception {
        String passphrase = "";
        createKeystore(passphrase, "foo", "bar");
        terminal.addTextInput("");
        // will not be prompted for a passphrase
        execute("foo");
        assertSecureString("foo", "bar", passphrase);
    }

    void setInput(String inputStr) {
        input = new ByteArrayInputStream(inputStr.getBytes(StandardCharsets.UTF_8));
    }
}
