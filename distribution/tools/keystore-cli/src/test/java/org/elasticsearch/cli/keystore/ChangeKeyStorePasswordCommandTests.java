/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.keystore;

import joptsimple.OptionSet;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;

public class ChangeKeyStorePasswordCommandTests extends KeyStoreCommandTestCase {
    @Override
    protected Command newCommand() {
        return new ChangeKeyStorePasswordCommand() {
            @Override
            protected Environment createEnv(OptionSet options, ProcessInfo processInfo) throws UserException {
                return env;
            }
        };
    }

    public void testSetKeyStorePassword() throws Exception {
        assumeFalse("Cannot open unprotected keystore on FIPS JVM", inFipsJvm());
        createKeystore("");
        loadKeystore("");
        terminal.addSecretInput("thepassword");
        terminal.addSecretInput("thepassword");
        // Prompted twice for the new password, since we didn't have an existing password
        execute();
        loadKeystore("thepassword");
    }

    public void testChangeKeyStorePassword() throws Exception {
        createKeystore("theoldpassword");
        loadKeystore("theoldpassword");
        terminal.addSecretInput("theoldpassword");
        terminal.addSecretInput("the-better-password");
        terminal.addSecretInput("the-better-password");
        // Prompted thrice: Once for the existing and twice for the new password
        execute();
        loadKeystore("the-better-password");
    }

    public void testChangeKeyStorePasswordToEmpty() throws Exception {
        assumeFalse("Cannot set empty keystore password on FIPS JVM", inFipsJvm());
        createKeystore("theoldpassword");
        loadKeystore("theoldpassword");
        terminal.addSecretInput("theoldpassword");
        terminal.addSecretInput("");
        terminal.addSecretInput("");
        // Prompted thrice: Once for the existing and twice for the new password
        execute();
        loadKeystore("");
    }

    public void testChangeKeyStorePasswordWrongVerification() throws Exception {
        createKeystore("theoldpassword");
        loadKeystore("theoldpassword");
        terminal.addSecretInput("theoldpassword");
        terminal.addSecretInput("thepassword");
        terminal.addSecretInput("themisspelledpassword");
        // Prompted thrice: Once for the existing and twice for the new password
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(e.getMessage(), ExitCodes.DATA_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("Passwords are not equal, exiting"));
    }

    public void testChangeKeyStorePasswordWrongExistingPassword() throws Exception {
        createKeystore("theoldpassword");
        loadKeystore("theoldpassword");
        terminal.addSecretInput("theoldmisspelledpassword");
        // We'll only be prompted once (for the old password)
        UserException e = expectThrows(UserException.class, this::execute);
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
}
