/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli.keystore;

import joptsimple.OptionSet;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.env.Environment;

import java.io.CharArrayWriter;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class AddStringKeyStoreCommandTests extends KeyStoreCommandTestCase {
    @Override
    protected Command newCommand() {
        return new AddStringKeyStoreCommand() {
            @Override
            protected Environment createEnv(OptionSet options, ProcessInfo processInfo) throws UserException {
                return env;
            }
        };
    }

    public void testInvalidPassphrease() throws Exception {
        String password = "keystorepassword";
        createKeystore(password, "foo", "bar");
        terminal.addSecretInput("thewrongpassword");
        UserException e = expectThrows(UserException.class, () -> execute("foo2"));
        assertEquals(e.getMessage(), ExitCodes.DATA_ERROR, e.exitCode);
        if (inFipsJvm()) {
            assertThat(
                e.getMessage(),
                anyOf(
                    containsString("Provided keystore password was incorrect"),
                    containsString("Keystore has been corrupted or tampered with")
                )
            );
        } else {
            assertThat(e.getMessage(), containsString("Provided keystore password was incorrect"));
        }

    }

    public void testMissingPromptCreateWithoutPasswordWhenPrompted() throws Exception {
        assumeFalse("Cannot create unprotected keystore on FIPS JVM", inFipsJvm());
        terminal.addTextInput("y");
        terminal.addSecretInput("bar");
        execute("foo");
        assertSecureString("foo", "bar", "");
    }

    public void testMissingPromptCreateWithoutPasswordWithoutPromptIfForced() throws Exception {
        assumeFalse("Cannot create unprotected keystore on FIPS JVM", inFipsJvm());
        terminal.addSecretInput("bar");
        execute("-f", "foo");
        assertSecureString("foo", "bar", "");
    }

    public void testMissingNoCreate() throws Exception {
        terminal.addTextInput("n"); // explicit no
        execute("foo");
        assertNull(KeyStoreWrapper.load(env.configDir()));
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
        terminal.addSecretInput(password);
        terminal.addTextInput("y");
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
        KeyStoreWrapper.create().save(env.configDir(), password.toCharArray());
        terminal.addSecretInput(password);
        terminal.addSecretInput("secret value");
        execute("foo");
        assertSecureString("foo", "secret value", password);
    }

    public void testPromptForMultipleValues() throws Exception {
        final String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configDir(), password.toCharArray());
        terminal.addSecretInput(password);
        terminal.addSecretInput("bar1");
        terminal.addSecretInput("bar2");
        terminal.addSecretInput("bar3");
        execute("foo1", "foo2", "foo3");
        assertSecureString("foo1", "bar1", password);
        assertSecureString("foo2", "bar2", password);
        assertSecureString("foo3", "bar3", password);
    }

    public void testStdinShort() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configDir(), password.toCharArray());
        terminal.addSecretInput(password);
        terminal.addSecretInput("secret value 1");
        execute("-x", "foo");
        assertSecureString("foo", "secret value 1", password);
    }

    public void testStdinLong() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configDir(), password.toCharArray());
        terminal.addSecretInput(password);
        terminal.addSecretInput("secret value 2");
        execute("--stdin", "foo");
        assertSecureString("foo", "secret value 2", password);
    }

    public void testStdinNoInput() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configDir(), password.toCharArray());
        terminal.addSecretInput(password);
        terminal.addSecretInput("");
        execute("-x", "foo");
        assertSecureString("foo", "", password);
    }

    public void testStdinInputWithLineBreaks() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configDir(), password.toCharArray());
        terminal.addSecretInput(password);
        terminal.addSecretInput("Typedthisandhitenter\n");
        execute("-x", "foo");
        assertSecureString("foo", "Typedthisandhitenter", password);
    }

    public void testStdinInputWithCarriageReturn() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configDir(), password.toCharArray());
        terminal.addSecretInput(password);
        terminal.addSecretInput("Typedthisandhitenter\r\n");
        execute("-x", "foo");
        assertSecureString("foo", "Typedthisandhitenter", password);
    }

    public void testStdinWithMultipleValues() throws Exception {
        final String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configDir(), password.toCharArray());
        terminal.addSecretInput(password);
        terminal.addSecretInput("bar1");
        terminal.addSecretInput("bar2");
        terminal.addSecretInput("bar3");
        execute(randomFrom("-x", "--stdin"), "foo1", "foo2", "foo3");
        assertSecureString("foo1", "bar1", password);
        assertSecureString("foo2", "bar2", password);
        assertSecureString("foo3", "bar3", password);
    }

    public void testAddUtf8String() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configDir(), password.toCharArray());
        terminal.addSecretInput(password);
        final int stringSize = randomIntBetween(8, 16);
        try (CharArrayWriter secretChars = new CharArrayWriter(stringSize)) {
            for (int i = 0; i < stringSize; i++) {
                secretChars.write((char) randomIntBetween(129, 2048));
            }
            terminal.addSecretInput(secretChars.toString());
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
        assertThat(e.getMessage(), containsString("the setting names can not be empty"));
    }

    public void testSpecialCharacterInName() throws Exception {
        String password = randomAlphaOfLengthBetween(14, 24);
        createKeystore(password);
        terminal.addSecretInput(password);
        terminal.addSecretInput("value");
        final String key = randomAlphaOfLength(4) + '@' + randomAlphaOfLength(4);
        final UserException e = expectThrows(UserException.class, () -> execute(key));
        final String exceptionString = "Setting name [" + key + "] does not match the allowed setting name pattern [[A-Za-z0-9_\\-.]+]";
        assertThat(e, hasToString(containsString(exceptionString)));
    }

    public void testAddToUnprotectedKeystore() throws Exception {
        assumeFalse("Cannot create unprotected keystores in FIPS mode", inFipsJvm());
        String password = "";
        createKeystore(password, "foo", "bar");
        terminal.addTextInput("");
        // will not be prompted for a password
        execute("foo");
        assertSecureString("foo", "bar", password);
    }
}
