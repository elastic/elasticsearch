/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import org.elasticsearch.common.base.Charsets;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.*;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

public class ShieldFiles {

    private ShieldFiles() {}

    /**
     * This writer opens a temporary file instead of the specified path and
     * tries to move the create tempfile to specified path on close. If possible
     * this move is tried to be atomic, but it will fall back to just replace the
     * existing file if the atomic move fails.
     *
     * If the destination path exists, it is overwritten
     *
     * @param path The path of the destination file
     * @return
     * @throws IOException
     */
    public static final Writer openAtomicMoveWriter(final Path path) throws IOException {
        final Path tempFile = Files.createTempFile(path.getParent(), path.getFileName().toString(), "tmp");
        final Writer writer = Files.newBufferedWriter(tempFile, Charsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        return new Writer() {
            @Override
            public void write(char[] cbuf, int off, int len) throws IOException {
                writer.write(cbuf, off, len);
            }

            @Override
            public void flush() throws IOException {
                writer.flush();
            }

            @Override
            public void close() throws IOException {
                writer.close();
                // get original permissions
                boolean supportsPosixPermissions = false;
                Set<PosixFilePermission> posixFilePermissions = null;
                if (Files.exists(path)) {
                    supportsPosixPermissions = Files.getFileStore(path).supportsFileAttributeView(PosixFileAttributeView.class);
                    if (supportsPosixPermissions) {
                        posixFilePermissions = Files.getPosixFilePermissions(path);
                    }
                }
                try {
                    Files.move(tempFile, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tempFile, path, StandardCopyOption.REPLACE_EXISTING);
                }
                // restore original permissions
                if (supportsPosixPermissions && posixFilePermissions != null) {
                    Files.setPosixFilePermissions(path, posixFilePermissions);
                }
            }
        };
    }
}
