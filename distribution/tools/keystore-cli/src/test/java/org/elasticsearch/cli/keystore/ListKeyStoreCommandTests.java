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

import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ListKeyStoreCommandTests extends KeyStoreCommandTestCase {

    @Override
    protected Command newCommand() {
        return new ListKeyStoreCommand() {
            @Override
            protected Environment createEnv(OptionSet options, ProcessInfo processInfo) throws UserException {
                return env;
            }
        };
    }

    public void testMissing() throws Exception {
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("keystore not found"));
    }

    public void testEmpty() throws Exception {
        String password = getPossibleKeystorePassword();
        createKeystore(password);
        terminal.addSecretInput(password);
        execute();
        assertThat(terminal.getOutput(), containsString("keystore.seed"));
    }

    public void testOne() throws Exception {
        String password = getPossibleKeystorePassword();
        createKeystore(password, "foo", "bar");
        terminal.addSecretInput(password);
        execute();
        assertThat(terminal.getOutput().lines().toList(), equalTo(List.of("foo", "keystore.seed")));
    }

    public void testMultiple() throws Exception {
        String password = getPossibleKeystorePassword();
        createKeystore(password, "foo", "1", "baz", "2", "bar", "3");
        terminal.addSecretInput(password);
        execute();
        assertThat(terminal.getOutput().lines().toList(), equalTo(List.of("bar", "baz", "foo", "keystore.seed"))); // sorted
    }

    public void testListWithIncorrectPassword() throws Exception {
        String password = "keystorepassword";
        createKeystore(password, "foo", "bar");
        terminal.addSecretInput("thewrongkeystorepassword");
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

    public void testListWithUnprotectedKeystore() throws Exception {
        assumeFalse("Cannot open unprotected keystore on FIPS JVM", inFipsJvm());
        createKeystore("", "foo", "bar");
        execute();
        // Not prompted for a password
        assertThat(terminal.getOutput().lines().toList(), equalTo(List.of("foo", "keystore.seed")));
    }
}
