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

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class ChangeKeyStorePasswordCommandTests extends KeyStoreCommandTestCase {
    @Override
    protected Command newCommand() {
        return new ChangeKeyStorePasswordCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return env;
            }
        };
    }

    public void testSetKeyStorePassword() throws Exception {
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
        terminal.addSecretInput("thepassword");
        terminal.addSecretInput("thepassword");
        // Prompted thrice: Once for the existing and twice for the new password
        execute();
        loadKeystore("thepassword");
    }

    public void testChangeKeyStorePasswordToEmpty() throws Exception {
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
        assertThat(e.getMessage(), containsString("Provided keystore password was incorrect"));
    }
}
