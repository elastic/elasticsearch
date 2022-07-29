/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.archivers.zip.UnixStat;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCopyDetails;
import org.gradle.api.file.RegularFile;
import org.gradle.api.internal.file.CopyActionProcessingStreamAction;
import org.gradle.api.internal.file.archive.compression.ArchiveOutputStreamFactory;
import org.gradle.api.internal.file.archive.compression.Bzip2Archiver;
import org.gradle.api.internal.file.archive.compression.GzipArchiver;
import org.gradle.api.internal.file.archive.compression.SimpleCompressor;
import org.gradle.api.internal.file.copy.CopyAction;
import org.gradle.api.internal.file.copy.CopyActionProcessingStream;
import org.gradle.api.internal.file.copy.FileCopyDetailsInternal;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.WorkResult;
import org.gradle.api.tasks.WorkResults;
import org.gradle.api.tasks.bundling.Tar;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

/**
 * A custom archive task that assembles a tar archive that preserves symbolic links.
 *
 * This task is necessary because the built-in task {@link org.gradle.api.tasks.bundling.Tar} does not preserve symbolic links.
 */
public class SymbolicLinkPreservingTar extends Tar {

    @Override
    protected CopyAction createCopyAction() {
        final ArchiveOutputStreamFactory compressor = switch (getCompression()) {
            case BZIP2 -> Bzip2Archiver.getCompressor();
            case GZIP -> GzipArchiver.getCompressor();
            default -> new SimpleCompressor();
        };
        return new SymbolicLinkPreservingTarCopyAction(getArchiveFile(), compressor, isPreserveFileTimestamps());
    }

    private static class SymbolicLinkPreservingTarCopyAction implements CopyAction {

        private final Provider<RegularFile> tarFile;
        private final ArchiveOutputStreamFactory compressor;
        private final boolean isPreserveFileTimestamps;

        SymbolicLinkPreservingTarCopyAction(
            final Provider<RegularFile> tarFile,
            final ArchiveOutputStreamFactory compressor,
            final boolean isPreserveFileTimestamps
        ) {
            this.tarFile = tarFile;
            this.compressor = compressor;
            this.isPreserveFileTimestamps = isPreserveFileTimestamps;
        }

        @Override
        public WorkResult execute(final CopyActionProcessingStream stream) {
            try (
                OutputStream out = compressor.createArchiveOutputStream(tarFile.get().getAsFile());
                TarArchiveOutputStream tar = new TarArchiveOutputStream(out)
            ) {
                tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
                stream.process(new SymbolicLinkPreservingTarStreamAction(tar));
            } catch (final IOException e) {
                throw new GradleException("failed writing tar file [" + tarFile + "]", e);
            }

            return WorkResults.didWork(true);
        }

        private class SymbolicLinkPreservingTarStreamAction implements CopyActionProcessingStreamAction {

            private final TarArchiveOutputStream tar;

            /*
             * When Gradle walks the file tree, it will follow symbolic links. This means that if there is a symbolic link to a directory
             * in the source file tree, we could otherwise end up duplicating the entries below that directory in the resulting tar archive.
             * To avoid this, we track which symbolic links we have visited, and skip files that are children of symbolic links that we have
             * already visited.
             */
            private final Set<File> visitedSymbolicLinks = new HashSet<>();

            SymbolicLinkPreservingTarStreamAction(final TarArchiveOutputStream tar) {
                this.tar = tar;
            }

            @Override
            public void processFile(final FileCopyDetailsInternal details) {
                if (isChildOfVisitedSymbolicLink(details) == false) {
                    if (isSymbolicLink(details)) {
                        visitSymbolicLink(details);
                    } else if (details.isDirectory()) {
                        visitDirectory(details);
                    } else {
                        visitFile(details);
                    }
                }
            }

            private boolean isChildOfVisitedSymbolicLink(final FileCopyDetailsInternal details) {
                final File file;
                try {
                    file = details.getFile();
                } catch (final UnsupportedOperationException e) {
                    // we get invoked with stubbed details, there is no way to introspect this other than catching this exception
                    return false;
                }
                for (final File symbolicLink : visitedSymbolicLinks) {
                    if (isChildOf(symbolicLink, file)) return true;
                }
                return false;
            }

            private boolean isChildOf(final File directory, final File file) {
                return file.toPath().startsWith(directory.toPath());
            }

            private boolean isSymbolicLink(final FileCopyDetailsInternal details) {
                final File file;
                try {
                    file = details.getFile();
                } catch (final UnsupportedOperationException e) {
                    // we get invoked with stubbed details, there is no way to introspect this other than catching this exception
                    return false;
                }
                return Files.isSymbolicLink(file.toPath());
            }

            private void visitSymbolicLink(final FileCopyDetailsInternal details) {
                visitedSymbolicLinks.add(details.getFile());
                final TarArchiveEntry entry = new TarArchiveEntry(details.getRelativePath().getPathString(), TarConstants.LF_SYMLINK);
                entry.setModTime(getModTime(details));
                entry.setMode(UnixStat.LINK_FLAG | details.getMode());
                try {
                    entry.setLinkName(Files.readSymbolicLink(details.getFile().toPath()).toString());
                    tar.putArchiveEntry(entry);
                    tar.closeArchiveEntry();
                } catch (final IOException e) {
                    handleProcessingException(details, e);
                }
            }

            private void visitDirectory(final FileCopyDetailsInternal details) {
                final TarArchiveEntry entry = new TarArchiveEntry(details.getRelativePath().getPathString() + "/");
                entry.setModTime(getModTime(details));
                entry.setMode(UnixStat.DIR_FLAG | details.getMode());
                try {
                    tar.putArchiveEntry(entry);
                    tar.closeArchiveEntry();
                } catch (final IOException e) {
                    handleProcessingException(details, e);
                }
            }

            private void visitFile(final FileCopyDetailsInternal details) {
                final TarArchiveEntry entry = new TarArchiveEntry(details.getRelativePath().getPathString());
                entry.setModTime(getModTime(details));
                entry.setMode(UnixStat.FILE_FLAG | details.getMode());
                entry.setSize(details.getSize());
                try {
                    tar.putArchiveEntry(entry);
                    details.copyTo(tar);
                    tar.closeArchiveEntry();
                } catch (final IOException e) {
                    handleProcessingException(details, e);
                }
            }

            private void handleProcessingException(final FileCopyDetailsInternal details, final IOException e) {
                throw new GradleException("could not add [" + details + "] to tar file [" + tarFile + "]", e);
            }

        }

        private long getModTime(final FileCopyDetails details) {
            return isPreserveFileTimestamps ? details.getLastModified() : 0;
        }

    }

}
