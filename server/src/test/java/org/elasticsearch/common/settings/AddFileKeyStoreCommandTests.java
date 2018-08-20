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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import static org.hamcrest.Matchers.containsString;

public class AddFileKeyStoreCommandTests extends KeyStoreCommandTestCase {
    @Override
    protected Command newCommand() {
        return new AddFileKeyStoreCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return env;
            }
        };
    }

    private Path createRandomFile() throws IOException {
        int length = randomIntBetween(10, 20);
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = randomByte();
        }
        Path file = env.configFile().resolve("randomfile");
        Files.write(file, bytes);
        return file;
    }

    private void addFile(KeyStoreWrapper keystore, String setting, Path file) throws Exception {
        keystore.setFile(setting, Files.readAllBytes(file));
        keystore.save(env.configFile(), new char[0]);
    }

    public void testMissingPromptCreate() throws Exception {
        Path file1 = createRandomFile();
        terminal.addTextInput("y");
        execute("foo", file1.toString());
        assertSecureFile("foo", file1);
    }

    public void testMissingForceCreate() throws Exception {
        Path file1 = createRandomFile();
        terminal.addSecretInput("bar");
        execute("-f", "foo", file1.toString());
        assertSecureFile("foo", file1);
    }

    public void testMissingNoCreate() throws Exception {
        terminal.addTextInput("n"); // explicit no
        execute("foo");
        assertNull(KeyStoreWrapper.load(env.configFile()));
    }

    public void testOverwritePromptDefault() throws Exception {
        Path file = createRandomFile();
        KeyStoreWrapper keystore = createKeystore("");
        addFile(keystore, "foo", file);
        terminal.addTextInput("");
        execute("foo", "path/dne");
        assertSecureFile("foo", file);
    }

    public void testOverwritePromptExplicitNo() throws Exception {
        Path file = createRandomFile();
        KeyStoreWrapper keystore = createKeystore("");
        addFile(keystore, "foo", file);
        terminal.addTextInput("n"); // explicit no
        execute("foo", "path/dne");
        assertSecureFile("foo", file);
    }

    public void testOverwritePromptExplicitYes() throws Exception {
        Path file1 = createRandomFile();
        KeyStoreWrapper keystore = createKeystore("");
        addFile(keystore, "foo", file1);
        terminal.addTextInput("y");
        Path file2 = createRandomFile();
        execute("foo", file2.toString());
        assertSecureFile("foo", file2);
    }

    public void testOverwriteForceShort() throws Exception {
        Path file1 = createRandomFile();
        KeyStoreWrapper keystore = createKeystore("");
        addFile(keystore, "foo", file1);
        Path file2 = createRandomFile();
        execute("-f", "foo", file2.toString());
        assertSecureFile("foo", file2);
    }

    public void testOverwriteForceLong() throws Exception {
        Path file1 = createRandomFile();
        KeyStoreWrapper keystore = createKeystore("");
        addFile(keystore, "foo", file1);
        Path file2 = createRandomFile();
        execute("--force", "foo", file2.toString());
        assertSecureFile("foo", file2);
    }

    public void testForceNonExistent() throws Exception {
        createKeystore("");
        Path file = createRandomFile();
        execute("--force", "foo", file.toString());
        assertSecureFile("foo", file);
    }

    public void testMissingSettingName() throws Exception {
        createKeystore("");
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("Missing setting name"));
    }

    public void testMissingFileName() throws Exception {
        createKeystore("");
        UserException e = expectThrows(UserException.class, () -> execute("foo"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("Missing file name"));
    }

    public void testFileDNE() throws Exception {
        createKeystore("");
        UserException e = expectThrows(UserException.class, () -> execute("foo", "path/dne"));
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("File [path/dne] does not exist"));
    }

    public void testExtraArguments() throws Exception {
        createKeystore("");
        Path file = createRandomFile();
        UserException e = expectThrows(UserException.class, () -> execute("foo", file.toString(), "bar"));
        assertEquals(e.getMessage(), ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("Unrecognized extra arguments [bar]"));
    }
}
