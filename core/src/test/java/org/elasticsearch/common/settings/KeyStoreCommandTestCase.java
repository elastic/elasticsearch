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
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.junit.After;
import org.junit.Before;

/**
 * Base test case for manipulating the ES keystore.
 */
@LuceneTestCase.SuppressFileSystems("*") // we do our own mocking
public abstract class KeyStoreCommandTestCase extends CommandTestCase {

    Environment env;

    List<FileSystem> fileSystems = new ArrayList<>();

    @After
    public void closeMockFileSystems() throws IOException {
        IOUtils.close(fileSystems);
    }

    @Before
    public void setupEnv() throws IOException {
        env = setupEnv(true, fileSystems); // default to posix, but tests may call setupEnv(false) to overwrite
    }

    public static Environment setupEnv(boolean posix, List<FileSystem> fileSystems) throws IOException {
        final Configuration configuration;
        if (posix) {
            configuration = Configuration.unix().toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build();
        } else {
            configuration = Configuration.unix();
        }
        FileSystem fs = Jimfs.newFileSystem(configuration);
        fileSystems.add(fs);
        PathUtilsForTesting.installMock(fs); // restored by restoreFileSystem in ESTestCase
        Path home = fs.getPath("/", "test-home");
        Files.createDirectories(home.resolve("config"));
        return TestEnvironment.newEnvironment(Settings.builder().put("path.home", home).build());
    }

    KeyStoreWrapper createKeystore(String password, String... settings) throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create(password.toCharArray());
        assertEquals(0, settings.length % 2);
        for (int i = 0; i < settings.length; i += 2) {
            keystore.setString(settings[i], settings[i + 1].toCharArray());
        }
        keystore.save(env.configFile());
        return keystore;
    }

    KeyStoreWrapper loadKeystore(String password) throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.load(env.configFile());
        keystore.decrypt(password.toCharArray());
        return keystore;
    }

    void assertSecureString(String setting, String value) throws Exception {
        assertSecureString(loadKeystore(""), setting, value);
    }

    void assertSecureString(KeyStoreWrapper keystore, String setting, String value) throws Exception {
        assertEquals(value, keystore.getString(setting).toString());
    }

    void assertSecureFile(String setting, Path file) throws Exception {
        assertSecureFile(loadKeystore(""), setting, file);
    }

    void assertSecureFile(KeyStoreWrapper keystore, String setting, Path file) throws Exception {
        byte[] expectedBytes = Files.readAllBytes(file);
        try (InputStream input = keystore.getFile(setting)) {
            for (int i = 0; i < expectedBytes.length; ++i) {
                int got = input.read();
                int expected = Byte.toUnsignedInt(expectedBytes[i]);
                if (got < 0) {
                    fail("Got EOF from keystore stream at position " + i + " but expected 0x" + Integer.toHexString(expected));
                }
                assertEquals("Byte " + i, expected, got);
            }
            int eof = input.read();
            if (eof != -1) {
                fail("Found extra bytes in file stream from keystore, expected " + expectedBytes.length +
                     " bytes but found 0x" + Integer.toHexString(eof));
            }
        }

    }
}
