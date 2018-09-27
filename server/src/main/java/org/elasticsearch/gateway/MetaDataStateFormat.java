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
package org.elasticsearch.gateway;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * MetaDataStateFormat is a base class to write checksummed
 * XContent based files to one or more directories in a standardized directory structure.
 * @param <T> the type of the XContent base data-structure
 */
public abstract class MetaDataStateFormat<T> {
    public static final XContentType FORMAT = XContentType.SMILE;
    public static final String STATE_DIR_NAME = "_state";
    public static final String STATE_FILE_EXTENSION = ".st";

    private static final String STATE_FILE_CODEC = "state";
    private static final int MIN_COMPATIBLE_STATE_FILE_VERSION = 1;
    private static final int STATE_FILE_VERSION = 1;
    private final String prefix;
    private final Pattern stateFilePattern;

    private static final Logger logger = Loggers.getLogger(MetaDataStateFormat.class);

    /**
     * Creates a new {@link MetaDataStateFormat} instance
     */
    protected MetaDataStateFormat(String prefix) {
        this.prefix = prefix;
        this.stateFilePattern = Pattern.compile(Pattern.quote(prefix) + "(\\d+)(" + MetaDataStateFormat.STATE_FILE_EXTENSION + ")?");
    }

    private static void deleteFileIfExists(Path stateLocation, Directory directory, String fileName) throws IOException {
        try {
            directory.deleteFile(fileName);
        } catch (FileNotFoundException | NoSuchFileException ignored) {

        }
        logger.trace("cleaned up {}", stateLocation.resolve(fileName));
    }

    private void writeStateToFirstLocation(final T state, Path stateLocation, Directory stateDir, String fileName, String tmpFileName)
            throws IOException {
        try {
            try (IndexOutput out = stateDir.createOutput(tmpFileName, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(out, STATE_FILE_CODEC, STATE_FILE_VERSION);
                out.writeInt(FORMAT.index());
                try (XContentBuilder builder = newXContentBuilder(FORMAT, new IndexOutputOutputStream(out) {
                    @Override
                    public void close() throws IOException {
                        // this is important since some of the XContentBuilders write bytes on close.
                        // in order to write the footer we need to prevent closing the actual index input.
                    }
                })) {

                    builder.startObject();
                    {
                        toXContent(builder, state);
                    }
                    builder.endObject();
                }
                CodecUtil.writeFooter(out);
            }

            stateDir.sync(Collections.singleton(tmpFileName));
            stateDir.rename(tmpFileName, fileName);
            stateDir.syncMetaData();
            logger.trace("written state to {}", stateLocation.resolve(fileName));
        } finally {
            deleteFileIfExists(stateLocation, stateDir, tmpFileName);
        }
    }

    private void copyStateToExtraLocation(Directory srcStateDir, Path extraStateLocation, String fileName, String tmpFileName)
            throws IOException {
        try (Directory extraStateDir = newDirectory(extraStateLocation)) {
            try {
                extraStateDir.copyFrom(srcStateDir, fileName, tmpFileName, IOContext.DEFAULT);
                extraStateDir.sync(Collections.singleton(tmpFileName));
                extraStateDir.rename(tmpFileName, fileName);
                extraStateDir.syncMetaData();
                logger.trace("copied state to {}", extraStateLocation.resolve(fileName));
            } finally {
                deleteFileIfExists(extraStateLocation, extraStateDir, tmpFileName);
            }
        }
    }

    /**
     * Writes the given state to the given directories. The state is written to a
     * state directory ({@value #STATE_DIR_NAME}) underneath each of the given file locations and is created if it
     * doesn't exist. The state is serialized to a temporary file in that directory and is then atomically moved to
     * it's target filename of the pattern {@code {prefix}{version}.st}.
     * If this method returns without exception there is a guarantee that state is persisted to the disk and loadLatestState will return it.
     * But if this method throws an exception, loadLatestState could return this state or some previous state.
     *
     * @param state     the state object to write
     * @param locations the locations where the state should be written to.
     * @throws IOException if an IOException occurs
     */
    public final void write(final T state, final Path... locations) throws IOException {
        if (locations == null) {
            throw new IllegalArgumentException("Locations must not be null");
        }
        if (locations.length <= 0) {
            throw new IllegalArgumentException("One or more locations required");
        }
        final long maxStateId = findMaxStateId(prefix, locations) + 1;
        assert maxStateId >= 0 : "maxStateId must be positive but was: [" + maxStateId + "]";

        final String fileName = prefix + maxStateId + STATE_FILE_EXTENSION;
        final String tmpFileName = fileName + ".tmp";
        final Path firstStateLocation = locations[0].resolve(STATE_DIR_NAME);
        try (Directory stateDir = newDirectory(firstStateLocation)) {
            writeStateToFirstLocation(state, firstStateLocation, stateDir, fileName, tmpFileName);

            for (int i = 1; i < locations.length; i++) {
                final Path extraStateLocation = locations[i].resolve(STATE_DIR_NAME);
                copyStateToExtraLocation(stateDir, extraStateLocation, fileName, tmpFileName);
            }
        }

        cleanupOldFiles(fileName, locations);
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
    public final T read(NamedXContentRegistry namedXContentRegistry, Path file) throws IOException {
        try (Directory dir = newDirectory(file.getParent())) {
            try (IndexInput indexInput = dir.openInput(file.getFileName().toString(), IOContext.DEFAULT)) {
                 // We checksum the entire file before we even go and parse it. If it's corrupted we barf right here.
                CodecUtil.checksumEntireFile(indexInput);
                CodecUtil.checkHeader(indexInput, STATE_FILE_CODEC, MIN_COMPATIBLE_STATE_FILE_VERSION, STATE_FILE_VERSION);
                final XContentType xContentType = XContentType.values()[indexInput.readInt()];
                if (xContentType != FORMAT) {
                    throw new IllegalStateException("expected state in " + file + " to be " + FORMAT + " format but was " + xContentType);
                }
                long filePointer = indexInput.getFilePointer();
                long contentSize = indexInput.length() - CodecUtil.footerLength() - filePointer;
                try (IndexInput slice = indexInput.slice("state_xcontent", filePointer, contentSize)) {
                    try (XContentParser parser = XContentFactory.xContent(FORMAT)
                            .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE,
                                new InputStreamIndexInput(slice, contentSize))) {
                        return fromXContent(parser);
                    }
                }
            } catch(CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                // we trick this into a dedicated exception with the original stacktrace
                throw new CorruptStateException(ex);
            }
        }
    }

    protected Directory newDirectory(Path dir) throws IOException {
        return new SimpleFSDirectory(dir);
    }

    private void cleanupOldFiles(final String currentStateFile, Path[] locations) throws IOException {
        for (Path location : locations) {
            logger.trace("cleanupOldFiles: cleaning up {}", location);
            Path stateLocation = location.resolve(STATE_DIR_NAME);
            try (Directory stateDir = newDirectory(stateLocation)) {
                for (String file : stateDir.listAll()) {
                    if (file.startsWith(prefix) && file.equals(currentStateFile) == false) {
                        deleteFileIfExists(stateLocation, stateDir, file);
                    }
                }
            }
        }
    }

    long findMaxStateId(final String prefix, Path... locations) throws IOException {
        long maxId = -1;
        for (Path dataLocation : locations) {
            final Path resolve = dataLocation.resolve(STATE_DIR_NAME);
            if (Files.exists(resolve)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(resolve, prefix + "*")) {
                    for (Path stateFile : stream) {
                        final Matcher matcher = stateFilePattern.matcher(stateFile.getFileName().toString());
                        if (matcher.matches()) {
                            final long id = Long.parseLong(matcher.group(1));
                            maxId = Math.max(maxId, id);
                        }
                    }
                }
            }
        }
        return maxId;
    }

    /**
     * Tries to load the latest state from the given data-locations. It tries to load the latest state determined by
     * the states version from one or more data directories and if none of the latest states can be loaded an exception
     * is thrown to prevent accidentally loading a previous state and silently omitting the latest state.
     *
     * @param logger a logger instance
     * @param dataLocations the data-locations to try.
     * @return the latest state or <code>null</code> if no state was found.
     */
    public T loadLatestState(Logger logger, NamedXContentRegistry namedXContentRegistry, Path... dataLocations) throws IOException {
        List<PathAndStateId> files = new ArrayList<>();
        long maxStateId = -1;
        if (dataLocations != null) { // select all eligible files first
            for (Path dataLocation : dataLocations) {
                final Path stateDir = dataLocation.resolve(STATE_DIR_NAME);
                // now, iterate over the current versions, and find latest one
                // we don't check if the stateDir is present since it could be deleted
                // after the check. Also if there is a _state file and it's not a dir something is really wrong
                // we don't pass a glob since we need the group part for parsing
                try (DirectoryStream<Path> paths = Files.newDirectoryStream(stateDir)) {
                    for (Path stateFile : paths) {
                        final Matcher matcher = stateFilePattern.matcher(stateFile.getFileName().toString());
                        if (matcher.matches()) {
                            final long stateId = Long.parseLong(matcher.group(1));
                            maxStateId = Math.max(maxStateId, stateId);
                            PathAndStateId pav = new PathAndStateId(stateFile, stateId);
                            logger.trace("found state file: {}", pav);
                            files.add(pav);
                        }
                    }
                } catch (NoSuchFileException | FileNotFoundException ex) {
                    // no _state directory -- move on
                }
            }
        }
        // NOTE: we might have multiple version of the latest state if there are multiple data dirs.. for this case
        //       we iterate only over the ones with the max version.
        long finalMaxStateId = maxStateId;
        Collection<PathAndStateId> pathAndStateIds = files
                .stream()
                .filter(pathAndStateId -> pathAndStateId.id == finalMaxStateId)
                .collect(Collectors.toCollection(ArrayList::new));

        final List<Throwable> exceptions = new ArrayList<>();
        for (PathAndStateId pathAndStateId : pathAndStateIds) {
            try {
                T state = read(namedXContentRegistry, pathAndStateId.file);
                logger.trace("state id [{}] read from [{}]", pathAndStateId.id, pathAndStateId.file.getFileName());
                return state;
            } catch (Exception e) {
                exceptions.add(new IOException("failed to read " + pathAndStateId.toString(), e));
                logger.debug(() -> new ParameterizedMessage(
                        "{}: failed to read [{}], ignoring...", pathAndStateId.file.toAbsolutePath(), prefix), e);
            }
        }
        // if we reach this something went wrong
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
        if (files.size() > 0) {
            // We have some state files but none of them gave us a usable state
            throw new IllegalStateException("Could not find a state file to recover from among " + files);
        }
        return null;
    }

    /**
     * Internal struct-like class that holds the parsed state id and the file
     */
    private static class PathAndStateId {
        final Path file;
        final long id;

        private PathAndStateId(Path file, long id) {
            this.file = file;
            this.id = id;
        }

        @Override
        public String toString() {
            return "[id:" + id + ", file:" + file.toAbsolutePath() + "]";
        }
    }

    /**
     * Deletes all meta state directories recursively for the given data locations
     * @param dataLocations the data location to delete
     */
    public static void deleteMetaState(Path... dataLocations) throws IOException {
        Path[] stateDirectories = new Path[dataLocations.length];
        for (int i = 0; i < dataLocations.length; i++) {
            stateDirectories[i] = dataLocations[i].resolve(STATE_DIR_NAME);
        }
        IOUtils.rm(stateDirectories);
    }
}
