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

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class EvilKeyStoreWrapperTests extends ESTestCase {

    public void testWritePermissions() throws Exception {
        assumeFalse("requires POSIX file permissions", Constants.WINDOWS);
        final Path configDir = createTempDir();
        PosixFileAttributeView attrs = Files.getFileAttributeView(configDir, PosixFileAttributeView.class);
        if (attrs != null) {
            // don't rely on umask: ensure the keystore has minimal permissions
            attrs.setPermissions(PosixFilePermissions.fromString("r--r-----"));
        }
        try {
            final KeyStoreWrapper wrapper = KeyStoreWrapper.create();
            final UserException e = expectThrows(UserException.class, () -> wrapper.save(configDir, new char[0]));
            final String expected = String.format(
                Locale.ROOT,
                "unable to create temporary keystore at [%s], write permissions required for [%s] or run [elasticsearch-keystore upgrade]",
                configDir.resolve("elasticsearch.keystore.tmp"),
                configDir);
            assertThat(e, hasToString(containsString(expected)));
            assertThat(e.exitCode, equalTo(ExitCodes.CONFIG));
            assertThat(e.getCause(), instanceOf(AccessDeniedException.class));
        } finally {
            // so the test framework can cleanup
            attrs.setPermissions(PosixFilePermissions.fromString("rw-rw----"));
        }
    }

}
