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
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class RemoveSettingKeyStoreCommandTests extends KeyStoreCommandTestCase {

    @Override
    protected Command newCommand() {
        return new RemoveSettingKeyStoreCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return env;
            }
        };
    }

    public void testMissing() throws Exception {
        UserException e = expectThrows(UserException.class, () -> execute("foo"));
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("keystore not found"));
    }

    public void testNoSettings() throws Exception {
        createKeystore("");
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("Must supply at least one setting"));
    }

    public void testNonExistentSetting() throws Exception {
        createKeystore("");
        UserException e = expectThrows(UserException.class, () -> execute("foo"));
        assertEquals(ExitCodes.CONFIG, e.exitCode);
        assertThat(e.getMessage(), containsString("[foo] does not exist"));
    }

    public void testOne() throws Exception {
        createKeystore("", "foo", "bar");
        execute("foo");
        assertFalse(loadKeystore("").getSettingNames().contains("foo"));
    }

    public void testMany() throws Exception {
        createKeystore("", "foo", "1", "bar", "2", "baz", "3");
        execute("foo", "baz");
        Set<String> settings = loadKeystore("").getSettingNames();
        assertFalse(settings.contains("foo"));
        assertFalse(settings.contains("baz"));
        assertTrue(settings.contains("bar"));
        assertEquals(2, settings.size()); // account for keystore.seed too
    }
}
