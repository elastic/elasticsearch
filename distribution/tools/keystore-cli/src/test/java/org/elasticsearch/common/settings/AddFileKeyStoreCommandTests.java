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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.anyOf;
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
        Path file = env.configFile().resolve(randomAlphaOfLength(16));
        Files.write(file, bytes);
        return file;
    }

    private void addFile(KeyStoreWrapper keystore, String setting, Path file, String password) throws Exception {
        keystore.setFile(setting, Files.readAllBytes(file));
        keystore.save(env.configFile(), password.toCharArray());
    }

    public void testMissingCreateWithEmptyPasswordWhenPrompted() throws Exception {
        assumeFalse("Cannot create unprotected keystore on FIPS JVM", inFipsJvm());
        String password = "";
        Path file1 = createRandomFile();
        terminal.addTextInput("y");
        execute("foo", file1.toString());
        assertSecureFile("foo", file1, password);
    }

    public void testMissingCreateWithEmptyPasswordWithoutPromptIfForced() throws Exception {
        assumeFalse("Cannot create unprotected keystore on FIPS JVM", inFipsJvm());
        String password = "";
        Path file1 = createRandomFile();
        execute("-f", "foo", file1.toString());
        assertSecureFile("foo", file1, password);
    }

    public void testMissingNoCreate() throws Exception {
        terminal.addSecretInput(randomFrom("", "keystorepassword"));
        terminal.addTextInput("n"); // explicit no
        execute("foo");
        assertNull(KeyStoreWrapper.load(env.configFile()));
    }

    public void testOverwritePromptDefault() throws Exception {
        String password = "keystorepassword";
        Path file = createRandomFile();
        KeyStoreWrapper keystore = createKeystore(password);
        addFile(keystore, "foo", file, password);
        terminal.addSecretInput(password);
        terminal.addSecretInput(password);
        terminal.addTextInput("");
        execute("foo", "path/dne");
        assertSecureFile("foo", file, password);
    }

    public void testOverwritePromptExplicitNo() throws Exception {
        String password = "keystorepassword";
        Path file = createRandomFile();
        KeyStoreWrapper keystore = createKeystore(password);
        addFile(keystore, "foo", file, password);
        terminal.addSecretInput(password);
        terminal.addTextInput("n"); // explicit no
        execute("foo", "path/dne");
        assertSecureFile("foo", file, password);
    }

    public void testOverwritePromptExplicitYes() throws Exception {
        String password = "keystorepassword";
        Path file1 = createRandomFile();
        KeyStoreWrapper keystore = createKeystore(password);
        addFile(keystore, "foo", file1, password);
        terminal.addSecretInput(password);
        terminal.addSecretInput(password);
        terminal.addTextInput("y");
        Path file2 = createRandomFile();
        execute("foo", file2.toString());
        assertSecureFile("foo", file2, password);
    }

    public void testOverwriteForceShort() throws Exception {
        String password = "keystorepassword";
        Path file1 = createRandomFile();
        KeyStoreWrapper keystore = createKeystore(password);
        addFile(keystore, "foo", file1, password);
        Path file2 = createRandomFile();
        terminal.addSecretInput(password);
        terminal.addSecretInput(password);
        execute("-f", "foo", file2.toString());
        assertSecureFile("foo", file2, password);
    }

    public void testOverwriteForceLong() throws Exception {
        String password = "keystorepassword";
        Path file1 = createRandomFile();
        KeyStoreWrapper keystore = createKeystore(password);
        addFile(keystore, "foo", file1, password);
        Path file2 = createRandomFile();
        terminal.addSecretInput(password);
        execute("--force", "foo", file2.toString());
        assertSecureFile("foo", file2, password);
    }

    public void testForceDoesNotAlreadyExist() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        Path file = createRandomFile();
        terminal.addSecretInput(password);
        execute("--force", "foo", file.toString());
        assertSecureFile("foo", file, password);
    }

    public void testMissingSettingName() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        terminal.addSecretInput(password);
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("Missing setting name"));
    }

    public void testMissingFileName() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        terminal.addSecretInput(password);
        UserException e = expectThrows(UserException.class, () -> execute("foo"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("settings and filenames must come in pairs"));
    }

    public void testFileDNE() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        terminal.addSecretInput(password);
        UserException e = expectThrows(UserException.class, () -> execute("foo", "path/dne"));
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("File [path/dne] does not exist"));
    }

    public void testExtraArguments() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        Path file = createRandomFile();
        terminal.addSecretInput(password);
        UserException e = expectThrows(UserException.class, () -> execute("foo", file.toString(), "bar"));
        assertEquals(e.getMessage(), ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("settings and filenames must come in pairs"));
    }

    public void testIncorrectPassword() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        Path file = createRandomFile();
        terminal.addSecretInput("thewrongkeystorepassword");
        UserException e = expectThrows(UserException.class, () -> execute("foo", file.toString()));
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

    public void testAddToUnprotectedKeystore() throws Exception {
        assumeFalse("Cannot open unprotected keystore on FIPS JVM", inFipsJvm());
        String password = "";
        Path file = createRandomFile();
        KeyStoreWrapper keystore = createKeystore(password);
        addFile(keystore, "foo", file, password);
        terminal.addTextInput("");
        // will not be prompted for a password
        execute("foo", "path/dne");
        assertSecureFile("foo", file, password);
    }

    public void testAddMultipleFiles() throws Exception {
        final String password = "keystorepassword";
        createKeystore(password);
        final int n = randomIntBetween(1, 8);
        final List<Tuple<String, Path>> settingFilePairs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            settingFilePairs.add(Tuple.tuple("foo" + i, createRandomFile()));
        }
        terminal.addSecretInput(password);
        execute(settingFilePairs.stream().flatMap(t -> Stream.of(t.v1(), t.v2().toString())).toArray(String[]::new));
        for (int i = 0; i < n; i++) {
            assertSecureFile(settingFilePairs.get(i).v1(), settingFilePairs.get(i).v2(), password);
        }
    }

}
