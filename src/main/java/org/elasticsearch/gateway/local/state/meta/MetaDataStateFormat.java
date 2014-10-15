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
package org.elasticsearch.gateway.local.state.meta;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.*;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.xcontent.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MetaDataStateFormat is a base class to write checksummed
 * XContent based files to one or more directories in a standardized directory structure.
 * @param <T> the type of the XContent base data-structure
 */
public abstract class MetaDataStateFormat<T> {
    public static final String STATE_DIR_NAME = "_state";
    public static final String STATE_FILE_EXTENSION = ".st";
    private static final String STATE_FILE_CODEC = "state";
    private static final int STATE_FILE_VERSION = 0;
    private static final int BUFFER_SIZE = 4096;
    private final XContentType format;
    private final boolean deleteOldFiles;

    /**
     * Creates a new {@link MetaDataStateFormat} instance
     * @param format the format of the x-content
     * @param deleteOldFiles if <code>true</code> write operations will
     *                       clean up old files written with this format.
     */
    protected MetaDataStateFormat(XContentType format, boolean deleteOldFiles) {
        this.format = format;
        this.deleteOldFiles = deleteOldFiles;
    }

    /**
     * Returns the {@link XContentType} used to serialize xcontent on write.
     */
    public XContentType format() {
        return format;
    }

    /**
     * Writes the given state to the given directories. The state is written to a
     * state directory ({@value #STATE_DIR_NAME}) underneath each of the given file locations and is created if it
     * doesn't exist. The state is serialized to a temporary file in that directory and is then atomically moved to
     * it's target filename of the pattern <tt>{prefix}{version}.st</tt>.
     *
     * @param state the state object to write
     * @param prefix the state names prefix used to compose the file name.
     * @param version the version of the state
     * @param locations the locations where the state should be written to.
     * @throws IOException if an IOException occurs
     */
    public final void write(final T state, final String prefix, final long version, final File... locations) throws IOException {
        Preconditions.checkArgument(locations != null, "Locations must not be null");
        Preconditions.checkArgument(locations.length > 0, "One or more locations required");
        String fileName = prefix + version + STATE_FILE_EXTENSION;
        File stateLocation = new File(locations[0], STATE_DIR_NAME);
        FileSystemUtils.mkdirs(stateLocation);
        final File tmpStateFile = new File(stateLocation, fileName + ".tmp");
        final Path tmpStatePath = tmpStateFile.toPath();
        final Path finalStatePath = new File(stateLocation, fileName).toPath();
        try {
            try (OutputStreamIndexOutput out = new OutputStreamIndexOutput(new FileOutputStream(tmpStateFile), BUFFER_SIZE)) {
                CodecUtil.writeHeader(out, STATE_FILE_CODEC, STATE_FILE_VERSION);
                out.writeInt(format.index());
                out.writeLong(version);
                try (XContentBuilder builder = newXContentBuilder(format, new org.elasticsearch.common.lucene.store.OutputStreamIndexOutput(out) {
                    @Override
                    public void close() throws IOException {
                        // this is important since some of the XContentBuilders write bytes on close.
                        // in order to write the footer we need to prevent closing the actual index input.
                    } })) {

                    builder.startObject();
                    {
                        toXContent(builder, state);
                    }
                    builder.endObject();
                }
                CodecUtil.writeFooter(out);
            }
            IOUtils.fsync(tmpStateFile, false); // fsync the state file
            Files.move(tmpStatePath, finalStatePath, StandardCopyOption.ATOMIC_MOVE);
            IOUtils.fsync(stateLocation, true);
            for (int i = 1; i < locations.length; i++) {
                stateLocation = new File(locations[i], STATE_DIR_NAME);
                FileSystemUtils.mkdirs(stateLocation);
                Path tmpPath = new File(stateLocation, fileName + ".tmp").toPath();
                Path finalPath = new File(stateLocation, fileName).toPath();
                try {
                    Files.copy(finalStatePath, tmpPath);
                    Files.move(tmpPath, finalPath, StandardCopyOption.ATOMIC_MOVE); // we are on the same FileSystem / Partition here we can do an atomic move
                    IOUtils.fsync(stateLocation, true); // we just fsync the dir here..
                } finally {
                    Files.deleteIfExists(tmpPath);
                }
            }
        } finally {
            Files.deleteIfExists(tmpStateFile.toPath());
        }
        if (deleteOldFiles) {
            cleanupOldFiles(prefix, fileName, locations);
        }
    }

    protected XContentBuilder newXContentBuilder(XContentType type, OutputStream stream ) throws IOException {
        return XContentFactory.contentBuilder(type, stream);
    }

    /**
     * Writes the given state to the given XContentBuilder
     * Subclasses need to implement this class for theirs specific state.
     */
    public abstract void toXContent(XContentBuilder builder, T state) throws IOException;

    /**
     * Reads a new instance of the state from the given XContentParser
     * Subclasses need to implement this class for theirs specific state.
     */
    public abstract T fromXContent(XContentParser parser) throws IOException;

    /**
     * Reads the state from a given file and compares the expected version against the actual version of
     * the state.
     */
    public final T read(File file, long expectedVersion) throws IOException {
        try (Directory dir = newDirectory(file.getParentFile())) {
            try (final IndexInput indexInput = dir.openInput(file.getName(), IOContext.DEFAULT)) {
                 // We checksum the entire file before we even go and parse it. If it's corrupted we barf right here.
                CodecUtil.checksumEntireFile(indexInput);
                CodecUtil.checkHeader(indexInput, STATE_FILE_CODEC, STATE_FILE_VERSION, STATE_FILE_VERSION);
                final XContentType xContentType = XContentType.values()[indexInput.readInt()];
                final long version = indexInput.readLong();
                if (version != expectedVersion) {
                    throw new CorruptStateException("State version mismatch expected: " + expectedVersion + " but was: " + version);
                }
                long filePointer = indexInput.getFilePointer();
                long contentSize = indexInput.length() - CodecUtil.footerLength() - filePointer;
                try (IndexInput slice = indexInput.slice("state_xcontent", filePointer, contentSize)) {
                    try (XContentParser parser = XContentFactory.xContent(xContentType).createParser(new InputStreamIndexInput(slice, contentSize))) {
                        return fromXContent(parser);
                    }
                }
            } catch(CorruptIndexException ex) {
                // we trick this into a dedicated exception with the original stacktrace
                throw new CorruptStateException(ex);
            }
        }
    }

    protected Directory newDirectory(File dir) throws IOException {
        return new SimpleFSDirectory(dir);
    }

    private void cleanupOldFiles(String prefix, String fileName, File[] locations) throws IOException {
        // now clean up the old files
        for (File dataLocation : locations) {
            final File[] files = new File(dataLocation, STATE_DIR_NAME).listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.getName().startsWith(prefix)) {
                        continue;
                    }
                    if (file.getName().equals(fileName)) {
                        continue;
                    }
                    Files.delete(file.toPath());
                }
            }
        }
    }

    /**
     * Tries to load the latest state from the given data-locations. It tries to load the latest state determined by
     * the states version from one or more data directories and if none of the latest states can be loaded an exception
     * is thrown to prevent accidentally loading a previous state and silently omitting the latest state.
     *
     * @param logger an elasticsearch logger instance
     * @param format the actual metastate format to use
     * @param pattern the file name pattern to identify files belonging to this pattern and to read the version from.
     *                The first capture group should return the version of the file. If the second capture group is has a
     *                null value the files is considered a legacy file and will be treated as if the file contains a plain
     *                x-content payload.
     * @param stateType the state type we are loading. used for logging contenxt only.
     * @param dataLocations the data-locations to try.
     * @return the latest state or <code>null</code> if no state was found.
     */
    public static <T> T loadLatestState(ESLogger logger, MetaDataStateFormat<T> format, Pattern pattern, String stateType, File... dataLocations) {
        List<FileAndVersion> files = new ArrayList<>();
        long maxVersion = -1;
        if (dataLocations != null) { // select all eligable files first
            for (File dataLocation : dataLocations) {
                File stateDir = new File(dataLocation, MetaDataStateFormat.STATE_DIR_NAME);
                if (!stateDir.exists() || !stateDir.isDirectory()) {
                    continue;
                }
                // now, iterate over the current versions, and find latest one
                File[] stateFiles = stateDir.listFiles();
                if (stateFiles == null) {
                    continue;
                }
                for (File stateFile : stateFiles) {
                    final Matcher matcher = pattern.matcher(stateFile.getName());
                    if (matcher.matches()) {
                        final long version = Long.parseLong(matcher.group(1));
                        maxVersion = Math.max(maxVersion, version);
                        final boolean legacy = MetaDataStateFormat.STATE_FILE_EXTENSION.equals(matcher.group(2)) == false;
                        files.add(new FileAndVersion(stateFile, version, legacy));
                    }
                }
            }
        }
        final List<Throwable> exceptions = new ArrayList<>();
        T state = null;
        // NOTE: we might have multiple version of the latest state if there are multiple data dirs.. for this case
        //       we iterate only over the ones with the max version
        for (FileAndVersion fileAndVersion : Collections2.filter(files, new VersionPredicate(maxVersion))) {
            try {
                final File stateFile = fileAndVersion.file;
                final long version = fileAndVersion.version;
                final XContentParser parser;
                if (fileAndVersion.legacy) { // read the legacy format -- plain XContent
                    try (FileInputStream stream = new FileInputStream(stateFile)) {
                        final byte[] data = Streams.copyToByteArray(stream);
                        if (data.length == 0) {
                            logger.debug("{}: no data for [{}], ignoring...", stateType, stateFile.getAbsolutePath());
                            continue;
                        }
                        parser = XContentHelper.createParser(data, 0, data.length);
                        state = format.fromXContent(parser);
                        if (state == null) {
                            logger.debug("{}: no data for [{}], ignoring...", stateType, stateFile.getAbsolutePath());
                        }
                    }
                } else {
                    state = format.read(stateFile, version);
                }
                return state;
            } catch (Throwable e) {
                exceptions.add(e);
                logger.debug("{}: failed to read [{}], ignoring...", e, fileAndVersion.file.getAbsolutePath(), stateType);
            }
        }
        // if we reach this something went wrong
        if (files.size() > 0 || exceptions.size() > 0) {
            // here we where not able to load the latest version from neither of the data dirs
            // this case is exceptional and we should not continue
            ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
        }
        return state;
    }

    /**
     * Filters out all {@link FileAndVersion} instances with a different version than
     * the given one.
     */
    private static final class VersionPredicate implements Predicate<FileAndVersion> {
        private final long version;

        VersionPredicate(long version) {
            this.version = version;
        }
        @Override
        public boolean apply(FileAndVersion input) {
            return input.version == version;
        }
    }

    /**
     * Internal struct-like class that holds the parsed state version, the file
     * and a flag if the file is a legacy state ie. pre 1.5
     */
    private static class FileAndVersion implements Comparable<FileAndVersion>{
        final File file;
        final long version;
        final boolean legacy;

        private FileAndVersion(File file, long version, boolean legacy) {
            this.file = file;
            this.version = version;
            this.legacy = legacy;
        }

        @Override
        public int compareTo(FileAndVersion o) {
            // highest first
            return Long.compare(o.version, version);
        }
    }

}
