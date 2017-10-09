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

import java.util.Map;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import static org.hamcrest.Matchers.containsString;

public class ListKeyStoreCommandTests extends KeyStoreCommandTestCase {

    @Override
    protected Command newCommand() {
        return new ListKeyStoreCommand() {
            @Override
            protected Environment createEnv(Terminal terminal, Map<String, String> settings) throws UserException {
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
        createKeystore("");
        execute();
        assertEquals("keystore.seed\n", terminal.getOutput());
    }

    public void testOne() throws Exception {
        createKeystore("", "foo", "bar");
        execute();
        assertEquals("foo\nkeystore.seed\n", terminal.getOutput());
    }

    public void testMultiple() throws Exception {
        createKeystore("", "foo", "1", "baz", "2", "bar", "3");
        execute();
        assertEquals("bar\nbaz\nfoo\nkeystore.seed\n", terminal.getOutput()); // sorted
    }
}
