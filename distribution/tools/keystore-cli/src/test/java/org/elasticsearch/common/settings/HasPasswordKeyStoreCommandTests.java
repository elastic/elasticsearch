/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.nullValue;

public class HasPasswordKeyStoreCommandTests extends KeyStoreCommandTestCase {
    @Override
    protected Command newCommand() {
        return new HasPasswordKeyStoreCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return env;
            }
        };
    }

    public void testFailsWithNoKeystore() throws Exception {
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals("Unexpected exit code", HasPasswordKeyStoreCommand.NO_PASSWORD_EXIT_CODE, e.exitCode);
        assertThat("Exception should have null message", e.getMessage(), is(nullValue()));
    }

    public void testFailsWhenKeystoreLacksPassword() throws Exception {
        assumeFalse("Cannot create unprotected keystores in FIPS mode", inFipsJvm());
        createKeystore("");
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals("Unexpected exit code", HasPasswordKeyStoreCommand.NO_PASSWORD_EXIT_CODE, e.exitCode);
        assertThat("Exception should have null message", e.getMessage(), is(nullValue()));
    }

    public void testSucceedsWhenKeystoreHasPassword() throws Exception {
        createKeystore("keystore-password");
        String output = execute();
        assertThat(output, containsString("Keystore is password-protected"));
    }

    public void testSilentSucceedsWhenKeystoreHasPassword() throws Exception {
        createKeystore("keystre-password");
        String output = execute("--silent");
        assertThat(output, is(emptyString()));
    }
}
