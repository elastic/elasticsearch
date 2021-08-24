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
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.cli.ShowKeyStoreCommand;
import org.elasticsearch.env.Environment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ShowKeyStoreCommandTests extends KeyStoreCommandTestCase {

    @Override
    protected Command newCommand() {
        return new ShowKeyStoreCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return env;
            }
        };
    }

    public void testErrorOnMissingKeystore() throws Exception {
        final UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("keystore not found"));
    }

    public void testErrorOnMissingParameter() throws Exception {
        createKeystore("");
        final UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("Must provide a single setting name to show"));
    }

    public void testErrorWhenSettingDoesNotExist() throws Exception {
        final String password = getPossibleKeystorePassword();
        createKeystore(password);
        terminal.addSecretInput(password);
        final UserException e = expectThrows(UserException.class, () -> execute("not.a.value"));
        assertEquals(ExitCodes.CONFIG, e.exitCode);
        assertThat(e.getMessage(), containsString("Setting [not.a.value] does not exist in the keystore"));
    }

    public void testPrintSingleValueToTerminal() throws Exception {
        final String password = getPossibleKeystorePassword();
        final String value = randomAlphaOfLengthBetween(6, 12);
        createKeystore(password, "reindex.ssl.keystore.password", value);
        terminal.addSecretInput(password);
        terminal.setHasOutputStream(false);
        execute("reindex.ssl.keystore.password");
        assertEquals(value + "\n", terminal.getOutput());
    }

    public void testShowBinaryValue() throws Exception {
        final String password = getPossibleKeystorePassword();
        final byte[] value = randomByteArrayOfLength(randomIntBetween(16, 2048));
        KeyStoreWrapper ks = createKeystore(password);
        ks.setFile("binary.file", value);
        saveKeystore(ks, password);

        terminal.addSecretInput(password);
        terminal.setHasOutputStream(true);

        execute("binary.file");
        assertThat(terminal.getOutputBytes(), equalTo(value));
    }

    public void testErrorIfOutputBinaryToTerminal() throws Exception {
        final String password = getPossibleKeystorePassword();
        final byte[] value = randomByteArrayOfLength(randomIntBetween(16, 2048));
        KeyStoreWrapper ks = createKeystore(password);
        ks.setFile("binary.file", value);
        saveKeystore(ks, password);

        terminal.addSecretInput(password);
        terminal.setHasOutputStream(false);

        UserException e = expectThrows(UserException.class, () -> execute("binary.file"));
        assertEquals(e.getMessage(), ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("Please redirect binary output to a file instead"));

    }

    public void testErrorOnIncorrectPassword() throws Exception {
        String password = "keystorepassword";
        createKeystore(password, "foo", "bar");
        terminal.addSecretInput("thewrongkeystorepassword");
        UserException e = expectThrows(UserException.class, () -> execute("keystore.seed"));
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

    public void testRetrieveFromUnprotectedKeystore() throws Exception {
        assumeFalse("Cannot open unprotected keystore on FIPS JVM", inFipsJvm());
        final String name = randomAlphaOfLengthBetween(6, 12);
        final String value = randomAlphaOfLengthBetween(6, 12);
        createKeystore("", name, value);
        final boolean console = randomBoolean();
        if (console) {
            terminal.setHasOutputStream(false);
        }

        execute(name);
        // Not prompted for a password

        if (console) {
            assertEquals(value + "\n", terminal.getOutput());
        } else {
            assertEquals(value, terminal.getOutput());
        }
    }
}
