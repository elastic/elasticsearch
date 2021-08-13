/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
