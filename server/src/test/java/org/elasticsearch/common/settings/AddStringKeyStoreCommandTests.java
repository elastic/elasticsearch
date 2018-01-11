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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import static org.hamcrest.Matchers.containsString;

public class AddStringKeyStoreCommandTests extends KeyStoreCommandTestCase {
    InputStream input;

    @Override
    protected Command newCommand() {
        return new AddStringKeyStoreCommand() {
            @Override
            protected Environment createEnv(Terminal terminal, Map<String, String> settings) throws UserException {
                return env;
            }
            @Override
            InputStream getStdin() {
                return input;
            }
        };
    }

    public void testMissing() throws Exception {
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("keystore not found"));
    }

    public void testOverwritePromptDefault() throws Exception {
        createKeystore("", "foo", "bar");
        terminal.addTextInput("");
        execute("foo");
        assertSecureString("foo", "bar");
    }

    public void testOverwritePromptExplicitNo() throws Exception {
        createKeystore("", "foo", "bar");
        terminal.addTextInput("n"); // explicit no
        execute("foo");
        assertSecureString("foo", "bar");
    }

    public void testOverwritePromptExplicitYes() throws Exception {
        createKeystore("", "foo", "bar");
        terminal.addTextInput("y");
        terminal.addSecretInput("newvalue");
        execute("foo");
        assertSecureString("foo", "newvalue");
    }

    public void testOverwriteForceShort() throws Exception {
        createKeystore("", "foo", "bar");
        terminal.addSecretInput("newvalue");
        execute("-f", "foo"); // force
        assertSecureString("foo", "newvalue");
    }

    public void testOverwriteForceLong() throws Exception {
        createKeystore("", "foo", "bar");
        terminal.addSecretInput("and yet another secret value");
        execute("--force", "foo"); // force
        assertSecureString("foo", "and yet another secret value");
    }

    public void testForceNonExistent() throws Exception {
        createKeystore("");
        terminal.addSecretInput("value");
        execute("--force", "foo"); // force
        assertSecureString("foo", "value");
    }

    public void testPromptForValue() throws Exception {
        KeyStoreWrapper.create(new char[0]).save(env.configFile());
        terminal.addSecretInput("secret value");
        execute("foo");
        assertSecureString("foo", "secret value");
    }

    public void testStdinShort() throws Exception {
        KeyStoreWrapper.create(new char[0]).save(env.configFile());
        setInput("secret value 1");
        execute("-x", "foo");
        assertSecureString("foo", "secret value 1");
    }

    public void testStdinLong() throws Exception {
        KeyStoreWrapper.create(new char[0]).save(env.configFile());
        setInput("secret value 2");
        execute("--stdin", "foo");
        assertSecureString("foo", "secret value 2");
    }

    public void testNonAsciiValue() throws Exception {
        KeyStoreWrapper.create(new char[0]).save(env.configFile());
        terminal.addSecretInput("non-äsčîï");
        UserException e = expectThrows(UserException.class, () -> execute("foo"));
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertEquals("String value must contain only ASCII", e.getMessage());
    }

    public void testMissingSettingName() throws Exception {
        createKeystore("");
        terminal.addTextInput("");
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("The setting name can not be null"));
    }

    void setInput(String inputStr) {
        input = new ByteArrayInputStream(inputStr.getBytes(StandardCharsets.UTF_8));
    }
}
