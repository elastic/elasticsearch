/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.io.ByteArrayInputStream;
import java.io.CharArrayWriter;
import java.nio.charset.Charset;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class AddStringKeyStoreCommandTests extends KeyStoreCommandTestCase {

    @Override
    protected Command newCommand() {
        return new AddStringKeyStoreCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return env;
            }
        };
    }

    public void testInvalidPassphrase() throws Exception {
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

    public void testPromptForMultipleValues() throws Exception {
        final String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
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
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        Terminal.SystemTerminal systemTerminal = new Terminal.SystemTerminal(
            new ByteArrayInputStream((password + System.lineSeparator() + "secret value 1").getBytes(Charset.defaultCharset()))
        );
        newCommand().mainWithoutErrorHandling(new String[] { "-x", "foo" }, systemTerminal);
        assertSecureString("foo", "secret value 1", password);
    }

    public void testStdinLong() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        Terminal.SystemTerminal systemTerminal = new Terminal.SystemTerminal(
            new ByteArrayInputStream((password + System.lineSeparator() + "secret value 2").getBytes(Charset.defaultCharset()))
        );
        newCommand().mainWithoutErrorHandling(new String[] { "--stdin", "foo" }, systemTerminal);
        assertSecureString("foo", "secret value 2", password);
    }

    public void testStdinNoInput() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        Terminal.SystemTerminal systemTerminal = new Terminal.SystemTerminal(
            new ByteArrayInputStream((password + System.lineSeparator() + "").getBytes(Charset.defaultCharset()))
        );
        newCommand().mainWithoutErrorHandling(new String[] { "-x", "foo" }, systemTerminal);
        assertSecureString("foo", "", password);
    }

    public void testStdinInputWithLineBreaks() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        Terminal.SystemTerminal systemTerminal = new Terminal.SystemTerminal(
            new ByteArrayInputStream((password + System.lineSeparator() + "Typedthisandhitenter\n").getBytes(Charset.defaultCharset()))
        );
        newCommand().mainWithoutErrorHandling(new String[] { "-x", "foo" }, systemTerminal);
        assertSecureString("foo", "Typedthisandhitenter", password);
    }

    public void testStdinInputWithCarriageReturn() throws Exception {
        String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        Terminal.SystemTerminal systemTerminal = new Terminal.SystemTerminal(
            new ByteArrayInputStream((password + System.lineSeparator() + "Typedthisandhitenter\r").getBytes(Charset.defaultCharset()))
        );
        newCommand().mainWithoutErrorHandling(new String[] { "-x", "foo" }, systemTerminal);
        assertSecureString("foo", "Typedthisandhitenter", password);
    }

    public void testStdinWithMultipleValues() throws Exception {
        final String password = "keystorepassword";
        KeyStoreWrapper.create().save(env.configFile(), password.toCharArray());
        Terminal.SystemTerminal systemTerminal = new Terminal.SystemTerminal(
            new ByteArrayInputStream((password + System.lineSeparator() + "bar1\nbar2\nbar3").getBytes(Charset.defaultCharset()))
        );
        newCommand().mainWithoutErrorHandling(new String[] { randomFrom("-x", "--stdin"), "foo1", "foo2", "foo3" }, systemTerminal);
        assertSecureString("foo1", "bar1", password);
        assertSecureString("foo2", "bar2", password);
        assertSecureString("foo3", "bar3", password);
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
