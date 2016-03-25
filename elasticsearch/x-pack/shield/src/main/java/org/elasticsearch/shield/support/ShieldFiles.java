/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;

public class ShieldFiles {

    private ShieldFiles() {
    }

    /**
     * This writer opens a temporary file instead of the specified path and
     * tries to move the create tempfile to specified path on close. If possible
     * this move is tried to be atomic, but it will fall back to just replace the
     * existing file if the atomic move fails.
     * <p>
     * If the destination path exists, it is overwritten
     *
     * @param path The path of the destination file
     */
    public static final Writer openAtomicMoveWriter(final Path path) throws IOException {
        final Path tempFile = Files.createTempFile(path.getParent(), path.getFileName().toString(), "tmp");
        final Writer writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption
                .TRUNCATE_EXISTING, StandardOpenOption.WRITE);
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
                if (Files.exists(path)) {
                    boolean supportsPosixAttributes =
                            Environment.getFileStore(path).supportsFileAttributeView(PosixFileAttributeView.class);
                    if (supportsPosixAttributes) {
                        setPosixAttributesOnTempFile(path, tempFile);
                    }
                }

                try {
                    Files.move(tempFile, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tempFile, path, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        };
    }

    static void setPosixAttributesOnTempFile(Path path, Path tempFile) throws IOException {
        PosixFileAttributes attributes = Files.getFileAttributeView(path, PosixFileAttributeView.class).readAttributes();
        PosixFileAttributeView tempFileView = Files.getFileAttributeView(tempFile, PosixFileAttributeView.class);

        tempFileView.setPermissions(attributes.permissions());

        // Make an attempt to set the username and group to match. If it fails, silently ignore the failure as the user
        // will be notified by the FileAttributeChecker that the ownership has changed and needs to be corrected
        try {
            tempFileView.setOwner(attributes.owner());
        } catch (Exception e) {
        }

        try {
            tempFileView.setGroup(attributes.group());
        } catch (Exception e) {
        }
    }
}
