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
        String password = "keystorepassword";
        createKeystore(password, "foo", "bar");
        terminal.addSecretInput("thewrongpassword");
        UserException e = expectThrows(UserException.class, () -> execute("foo2"));
        assertEquals(e.getMessage(), ExitCodes.DATA_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("Provided keystore password was incorrect"));

    }

    public void testMissingPromptCreateWithoutPasswordWhenPrompted() throws Exception {
        terminal.addTextInput("y");
        terminal.addSecretInput("bar");
        execute("foo");
        assertSecureString("foo", "bar", "");
    }

    public void testMissingPromptCreateWithoutPasswordWithoutPromptIfForced() throws Exception {
        terminal.addSecretInput("bar");
        execute("-f", "foo");
        assertSecureString("foo", "bar", "");
    }

    public void testMissingNoCreate() throws Exception {
        terminal.addTextInput("n"); // explicit no
        execute("foo");
        assertNull(KeyStoreWrapper.load(env.configFile()));
    }

    public void testOverwritePromptDefault() throws Exception {
        String password = "keystorepassword";
        createKeystore(password, "foo", "bar");
        terminal.addSecretInput(password);
        terminal.addTextInput("");
        execute("foo");
        assertSecureString("foo", "bar", password);
    }

    public void testOverwritePromptExplicitNo() throws Exception {
        String password = "keystorepassword";
        createKeystore(password, "foo", "bar");
        terminal.addSecretInput(password);
        terminal.addTextInput("n"); // explicit no
        execute("foo");
        assertSecureString("foo", "bar", password);
    }

    public void testOverwritePromptExplicitYes() throws Exception {
        String password = "keystorepassword";
        createKeystore(password, "foo", "bar");
        terminal.addTextInput("y");
        terminal.addSecretInput(password);
        terminal.addSecretInput("newvalue");
        execute("foo");
        assertSecureString("foo", "newvalue", password);
    }

    public void testOverwriteForceShort() throws Exception {
        String password = "keystorepassword";
        createKeystore(password, "foo", "bar");
        terminal.addSecretInput(password);
        terminal.addSecretInput("newvalue");
        execute("-f", "foo"); // force
        assertSecureString("foo", "newvalue", password);
    }

    public void testOverwriteForceLong() throws Exception {
        String password = "keystorepassword";
        createKeystore(password, "foo", "bar");
        terminal.addSecretInput(password);
        terminal.addSecretInput("and yet another secret value");
        execute("--force", "foo"); // force
        assertSecureString("foo", "and yet another secret value", password);
    }

    public void testForceNonExistent() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        terminal.addSecretInput(password);
        terminal.addSecretInput("value");
        execute("--force", "foo"); // force
        assertSecureString("foo", "value", password);
    }

    public void testPromptForValue() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        terminal.addSecretInput(password);
        terminal.addSecretInput("secret value");
        execute("foo");
        assertSecureString("foo", "secret value", password);
    }

    public void testStdinShort() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        terminal.addSecretInput(password);
        setInput("secret value 1");
        execute("-x", "foo");
        assertSecureString("foo", "secret value 1", password);
    }

    public void testStdinLong() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        terminal.addSecretInput(password);
        setInput("secret value 2");
        execute("--stdin", "foo");
        assertSecureString("foo", "secret value 2", password);
    }

    public void testStdinNoInput() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        terminal.addSecretInput(password);
        setInput("");
        execute("-x", "foo");
        assertSecureString("foo", "", password);
    }

    public void testStdinInputWithLineBreaks() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        terminal.addSecretInput(password);
        setInput("Typedthisandhitenter\n");
        execute("-x", "foo");
        assertSecureString("foo", "Typedthisandhitenter", password);
    }

    public void testStdinInputWithCarriageReturn() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        terminal.addSecretInput(password);
        setInput("Typedthisandhitenter\r");
        execute("-x", "foo");
        assertSecureString("foo", "Typedthisandhitenter", password);
    }

    public void testAddUtf8String() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        terminal.addSecretInput(password);
        final int stringSize = randomIntBetween(8, 16);
        try (CharArrayWriter secretChars = new CharArrayWriter(stringSize)) {
            for (int i = 0; i < stringSize; i++) {
                secretChars.write((char) randomIntBetween(129, 2048));
            }
            setInput(secretChars.toString());
            execute("-x", "foo");
            assertSecureString("foo", secretChars.toString(), password);
        }
    }

    public void testMissingSettingName() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        terminal.addSecretInput(password);
        terminal.addSecretInput(password);
        terminal.addTextInput("");
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("The setting name can not be null"));
    }

    public void testSpecialCharacterInName() throws Exception {
        createKeystore("");
        terminal.addSecretInput("value");
        final String key = randomAlphaOfLength(4) + '@' + randomAlphaOfLength(4);
        final UserException e = expectThrows(UserException.class, () -> execute(key));
        final String exceptionString = "Setting name [" + key + "] does not match the allowed setting name pattern [[A-Za-z0-9_\\-.]+]";
        assertThat(e, hasToString(containsString(exceptionString)));
    }

    public void testAddToUnprotectedKeystore() throws Exception {
        String password = "";
        createKeystore(password, "foo", "bar");
        terminal.addTextInput("");
        // will not be prompted for a password
        execute("foo");
        assertSecureString("foo", "bar", password);
    }

    void setInput(String inputStr) {
        input = new ByteArrayInputStream(inputStr.getBytes(StandardCharsets.UTF_8));
    }
}
