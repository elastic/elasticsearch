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
        createKeystore("");
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals("Unexpected exit code", HasPasswordKeyStoreCommand.NO_PASSWORD_EXIT_CODE, e.exitCode);
        assertThat("Exception should have null message", e.getMessage(), is(nullValue()));
    }

    public void testSucceedsWhenKeystoreHasPassword() throws Exception {
        createKeystore("password");
        String output = execute();
        assertThat(output, containsString("Keystore is password-protected"));
    }

    public void testSilentSucceedsWhenKeystoreHasPassword() throws Exception {
        createKeystore("password");
        String output = execute("--silent");
        assertThat(output, is(emptyString()));
    }
}
