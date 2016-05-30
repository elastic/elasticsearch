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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    private final String prefix;
    private final Pattern stateFilePattern;


    /**
     * Creates a new {@link MetaDataStateFormat} instance
     * @param format the format of the x-content
     */
    protected MetaDataStateFormat(XContentType format, String prefix) {
        this.format = format;
        this.prefix = prefix;
        this.stateFilePattern = Pattern.compile(Pattern.quote(prefix) + "(\\d+)(" + MetaDataStateFormat.STATE_FILE_EXTENSION + ")?");

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
     * @param version the version of the state
     * @param locations the locations where the state should be written to.
     * @throws IOException if an IOException occurs
     */
    public final void write(final T state, final long version, final Path... locations) throws IOException {
        if (locations == null) {
            throw new IllegalArgumentException("Locations must not be null");
        }
        if (locations.length <= 0) {
            throw new IllegalArgumentException("One or more locations required");
        }
        final long maxStateId = findMaxStateId(prefix, locations)+1;
        assert maxStateId >= 0 : "maxStateId must be positive but was: [" + maxStateId + "]";
        final String fileName = prefix + maxStateId + STATE_FILE_EXTENSION;
        Path stateLocation = locations[0].resolve(STATE_DIR_NAME);
        Files.createDirectories(stateLocation);
        final Path tmpStatePath = stateLocation.resolve(fileName + ".tmp");
        final Path finalStatePath = stateLocation.resolve(fileName);
        try {
            final String resourceDesc = "MetaDataStateFormat.write(path=\"" + tmpStatePath + "\")";
            try (OutputStreamIndexOutput out = new OutputStreamIndexOutput(resourceDesc, fileName, Files.newOutputStream(tmpStatePath), BUFFER_SIZE)) {
                CodecUtil.writeHeader(out, STATE_FILE_CODEC, STATE_FILE_VERSION);
                out.writeInt(format.index());
                out.writeLong(version);
                try (XContentBuilder builder = newXContentBuilder(format, new IndexOutputOutputStream(out) {
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
            IOUtils.fsync(tmpStatePath, false); // fsync the state file
            Files.move(tmpStatePath, finalStatePath, StandardCopyOption.ATOMIC_MOVE);
            IOUtils.fsync(stateLocation, true);
            for (int i = 1; i < locations.length; i++) {
                stateLocation = locations[i].resolve(STATE_DIR_NAME);
                Files.createDirectories(stateLocation);
                Path tmpPath = stateLocation.resolve(fileName + ".tmp");
                Path finalPath = stateLocation.resolve(fileName);
                try {
                    Files.copy(finalStatePath, tmpPath);
                    Files.move(tmpPath, finalPath, StandardCopyOption.ATOMIC_MOVE); // we are on the same FileSystem / Partition here we can do an atomic move
                    IOUtils.fsync(stateLocation, true); // we just fsync the dir here..
                } finally {
                    Files.deleteIfExists(tmpPath);
                }
            }
        } finally {
            Files.deleteIfExists(tmpStatePath);
        }
        cleanupOldFiles(prefix, fileName, locations);
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
    public final T read(Path file) throws IOException {
        try (Directory dir = newDirectory(file.getParent())) {
            try (final IndexInput indexInput = dir.openInput(file.getFileName().toString(), IOContext.DEFAULT)) {
                 // We checksum the entire file before we even go and parse it. If it's corrupted we barf right here.
                CodecUtil.checksumEntireFile(indexInput);
                CodecUtil.checkHeader(indexInput, STATE_FILE_CODEC, STATE_FILE_VERSION, STATE_FILE_VERSION);
                final XContentType xContentType = XContentType.values()[indexInput.readInt()];
                indexInput.readLong(); // version currently unused
                long filePointer = indexInput.getFilePointer();
                long contentSize = indexInput.length() - CodecUtil.footerLength() - filePointer;
                try (IndexInput slice = indexInput.slice("state_xcontent", filePointer, contentSize)) {
                    try (XContentParser parser = XContentFactory.xContent(xContentType).createParser(new InputStreamIndexInput(slice, contentSize))) {
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

    private void cleanupOldFiles(final String prefix, final String currentStateFile, Path[] locations) throws IOException {
        final DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() {
            @Override
            public boolean accept(Path entry) throws IOException {
                final String entryFileName = entry.getFileName().toString();
                return Files.isRegularFile(entry)
                        && entryFileName.startsWith(prefix) // only state files
                        && currentStateFile.equals(entryFileName) == false; // keep the current state file around
            }
        };
        // now clean up the old files
        for (Path dataLocation : locations) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataLocation.resolve(STATE_DIR_NAME), filter)) {
                for (Path stateFile : stream) {
                    Files.deleteIfExists(stateFile);
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
     * @param logger an elasticsearch logger instance
     * @param dataLocations the data-locations to try.
     * @return the latest state or <code>null</code> if no state was found.
     */
    public  T loadLatestState(ESLogger logger, Path... dataLocations) throws IOException {
        List<PathAndStateId> files = new ArrayList<>();
        long maxStateId = -1;
        boolean maxStateIdIsLegacy = true;
        if (dataLocations != null) { // select all eligible files first
            for (Path dataLocation : dataLocations) {
                final Path stateDir = dataLocation.resolve(STATE_DIR_NAME);
                // now, iterate over the current versions, and find latest one
                // we don't check if the stateDir is present since it could be deleted
                // after the check. Also if there is a _state file and it's not a dir something is really wrong
                try (DirectoryStream<Path> paths = Files.newDirectoryStream(stateDir)) { // we don't pass a glob since we need the group part for parsing
                    for (Path stateFile : paths) {
                        final Matcher matcher = stateFilePattern.matcher(stateFile.getFileName().toString());
                        if (matcher.matches()) {
                            final long stateId = Long.parseLong(matcher.group(1));
                            maxStateId = Math.max(maxStateId, stateId);
                            final boolean legacy = MetaDataStateFormat.STATE_FILE_EXTENSION.equals(matcher.group(2)) == false;
                            maxStateIdIsLegacy &= legacy; // on purpose, see NOTE below
                            PathAndStateId pav = new PathAndStateId(stateFile, stateId, legacy);
                            logger.trace("found state file: {}", pav);
                            files.add(pav);
                        }
                    }
                } catch (NoSuchFileException | FileNotFoundException ex) {
                    // no _state directory -- move on
                }
            }
        }
        final List<Throwable> exceptions = new ArrayList<>();
        T state = null;
        // NOTE: we might have multiple version of the latest state if there are multiple data dirs.. for this case
        //       we iterate only over the ones with the max version. If we have at least one state file that uses the
        //       new format (ie. legacy == false) then we know that the latest version state ought to use this new format.
        //       In case the state file with the latest version does not use the new format while older state files do,
        //       the list below will be empty and loading the state will fail
        Collection<PathAndStateId> pathAndStateIds = files
                .stream()
                .filter(new StateIdAndLegacyPredicate(maxStateId, maxStateIdIsLegacy))
                .collect(Collectors.toCollection(ArrayList::new));

        for (PathAndStateId pathAndStateId : pathAndStateIds) {
            try {
                final Path stateFile = pathAndStateId.file;
                final long id = pathAndStateId.id;
                if (pathAndStateId.legacy) { // read the legacy format -- plain XContent
                    final byte[] data = Files.readAllBytes(stateFile);
                    if (data.length == 0) {
                        logger.debug("{}: no data for [{}], ignoring...", prefix, stateFile.toAbsolutePath());
                        continue;
                    }
                    try (final XContentParser parser = XContentHelper.createParser(new BytesArray(data))) {
                        state = fromXContent(parser);
                    }
                    if (state == null) {
                        logger.debug("{}: no data for [{}], ignoring...", prefix, stateFile.toAbsolutePath());
                    }
                } else {
                    state = read(stateFile);
                    logger.trace("state id [{}] read from [{}]", id, stateFile.getFileName());
                }
                return state;
            } catch (Throwable e) {
                exceptions.add(new IOException("failed to read " + pathAndStateId.toString(), e));
                logger.debug("{}: failed to read [{}], ignoring...", e, pathAndStateId.file.toAbsolutePath(), prefix);
            }
        }
        // if we reach this something went wrong
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
        if (files.size() > 0) {
            // We have some state files but none of them gave us a usable state
            throw new IllegalStateException("Could not find a state file to recover from among " + files);
        }
        return state;
    }

    /**
     * Filters out all {@link org.elasticsearch.gateway.MetaDataStateFormat.PathAndStateId} instances with a different id than
     * the given one.
     */
    private static final class StateIdAndLegacyPredicate implements Predicate<PathAndStateId> {
        private final long id;
        private final boolean legacy;

        StateIdAndLegacyPredicate(long id, boolean legacy) {
            this.id = id;
            this.legacy = legacy;
        }

        @Override
        public boolean test(PathAndStateId input) {
            return input.id == id && input.legacy == legacy;
        }
    }

    /**
     * Internal struct-like class that holds the parsed state id, the file
     * and a flag if the file is a legacy state ie. pre 1.5
     */
    private static class PathAndStateId {
        final Path file;
        final long id;
        final boolean legacy;

        private PathAndStateId(Path file, long id, boolean legacy) {
            this.file = file;
            this.id = id;
            this.legacy = legacy;
        }

        @Override
        public String toString() {
            return "[id:" + id + ", legacy:" + legacy + ", file:" + file.toAbsolutePath() + "]";
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
