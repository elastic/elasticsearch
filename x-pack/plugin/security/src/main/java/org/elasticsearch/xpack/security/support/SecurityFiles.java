/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public class SecurityFiles {

    private SecurityFiles() {
    }

    /**
     * Atomically writes to the specified file a line per entry in the specified map using the specified transform to convert each entry to
     * a line. The writing is done atomically in the following sense: first the lines are written to a temporary file and if the writing
     * succeeds then the temporary file is moved to the specified path, replacing the file if it exists. If a failure occurs, any existing
     * file is preserved, and the temporary file is cleaned up.
     *
     * @param <K>       the key type of the map entries
     * @param <V>       the value type of the map entries
     * @param path      the path
     * @param map       the map whose entries to transform into lines
     * @param transform the transform to convert each map entry to a line
     */
    public static <K, V> void writeFileAtomically(final Path path, final Map<K, V> map, final Function<Map.Entry<K, V>, String> transform) {
        Path tempFile = null;
        try {
            tempFile = Files.createTempFile(path.getParent(), path.getFileName().toString(), "tmp");
            try (Writer writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8, CREATE, TRUNCATE_EXISTING, WRITE)) {
                for (final Map.Entry<K, V> entry : map.entrySet()) {
                    final StringBuilder sb = new StringBuilder();
                    final String line = sb.append(transform.apply(entry)).append(System.lineSeparator()).toString();
                    writer.write(line);
                }
            }
            // get original permissions
            if (Files.exists(path)) {
                boolean supportsPosixAttributes =
                        Environment.getFileStore(path).supportsFileAttributeView(PosixFileAttributeView.class);
                if (supportsPosixAttributes) {
                    setPosixAttributesOnTempFile(path, tempFile);
                }
            }

            try {
                Files.move(tempFile, path, REPLACE_EXISTING, ATOMIC_MOVE);
            } catch (final AtomicMoveNotSupportedException e) {
                Files.move(tempFile, path, REPLACE_EXISTING);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(String.format(Locale.ROOT, "could not write file [%s]", path.toAbsolutePath()), e);
        } finally {
            // we are ignoring exceptions here, so we do not need handle whether or not tempFile was initialized nor if the file exists
            IOUtils.deleteFilesIgnoringExceptions(tempFile);
        }
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
