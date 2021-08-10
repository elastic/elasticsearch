/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Tuple;
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
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MetadataStateFormat is a base class to write checksummed
 * XContent based files to one or more directories in a standardized directory structure.
 * @param <T> the type of the XContent base data-structure
 */
public abstract class MetadataStateFormat<T> {
    public static final XContentType FORMAT = XContentType.SMILE;
    public static final String STATE_DIR_NAME = "_state";
    public static final String STATE_FILE_EXTENSION = ".st";

    private static final String STATE_FILE_CODEC = "state";
    private static final int MIN_COMPATIBLE_STATE_FILE_VERSION = 1;
    private static final int STATE_FILE_VERSION = 1;
    private final String prefix;
    private final Pattern stateFilePattern;

    private static final Logger logger = LogManager.getLogger(MetadataStateFormat.class);

    /**
     * Creates a new {@link MetadataStateFormat} instance
     */
    protected MetadataStateFormat(String prefix) {
        this.prefix = prefix;
        this.stateFilePattern = Pattern.compile(Pattern.quote(prefix) + "(\\d+)(" + MetadataStateFormat.STATE_FILE_EXTENSION + ")?");
    }

    private static void deleteFileIfExists(Path stateLocation, Directory directory, String fileName) throws IOException {
        try {
            directory.deleteFile(fileName);
        } catch (FileNotFoundException | NoSuchFileException ignored) {

        }
        logger.trace("cleaned up {}", stateLocation.resolve(fileName));
    }

    private static void deleteFileIgnoreExceptions(Path stateLocation, Directory directory, String fileName) {
        try {
            deleteFileIfExists(stateLocation, directory, fileName);
        } catch (IOException e) {
            logger.trace("clean up failed {}", stateLocation.resolve(fileName));
        }
    }

    private void writeStateToFirstLocation(final T state, Path stateLocation, Directory stateDir, String tmpFileName)
            throws WriteStateException {
        try {
            deleteFileIfExists(stateLocation, stateDir, tmpFileName);
            try (IndexOutput out = EndiannessReverserUtil.createOutput(stateDir, tmpFileName, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(out, STATE_FILE_CODEC, STATE_FILE_VERSION);
                out.writeInt(FORMAT.index());
                try (XContentBuilder builder = newXContentBuilder(FORMAT, new IndexOutputOutputStream(out) {
                    @Override
                    public void close() {
                        // this is important since some of the XContentBuilders write bytes on close.
                        // in order to write the footer we need to prevent closing the actual index input.
                    }
                })) {
                    builder.startObject();
                    toXContent(builder, state);
                    builder.endObject();
                }
                CodecUtil.writeFooter(out);
            }

            stateDir.sync(Collections.singleton(tmpFileName));
        } catch (Exception e) {
            throw new WriteStateException(false, "failed to write state to the first location tmp file " +
                    stateLocation.resolve(tmpFileName), e);
        }
    }

    private static void copyStateToExtraLocations(List<Tuple<Path, Directory>> stateDirs, String tmpFileName)
            throws WriteStateException {
        Directory srcStateDir = stateDirs.get(0).v2();
        for (int i = 1; i < stateDirs.size(); i++) {
            Tuple<Path, Directory> extraStatePathAndDir = stateDirs.get(i);
            Path extraStateLocation = extraStatePathAndDir.v1();
            Directory extraStateDir = extraStatePathAndDir.v2();
            try {
                deleteFileIfExists(extraStateLocation, extraStateDir, tmpFileName);
                extraStateDir.copyFrom(srcStateDir, tmpFileName, tmpFileName, IOContext.DEFAULT);
                extraStateDir.sync(Collections.singleton(tmpFileName));
            } catch (Exception e) {
                throw new WriteStateException(false, "failed to copy tmp state file to extra location " + extraStateLocation, e);
            }
        }
    }

    private static void performRenames(String tmpFileName, String fileName, final List<Tuple<Path, Directory>> stateDirectories) throws
            WriteStateException {
        Directory firstStateDirectory = stateDirectories.get(0).v2();
        try {
            firstStateDirectory.rename(tmpFileName, fileName);
        } catch (IOException e) {
            throw new WriteStateException(false, "failed to rename tmp file to final name in the first state location " +
                    stateDirectories.get(0).v1().resolve(tmpFileName), e);
        }

        for (int i = 1; i < stateDirectories.size(); i++) {
            Directory extraStateDirectory = stateDirectories.get(i).v2();
            try {
                extraStateDirectory.rename(tmpFileName, fileName);
            } catch (IOException e) {
                throw new WriteStateException(true, "failed to rename tmp file to final name in extra state location " +
                        stateDirectories.get(i).v1().resolve(tmpFileName), e);
            }
        }
    }

    private static void performStateDirectoriesFsync(List<Tuple<Path, Directory>> stateDirectories) throws WriteStateException {
        for (int i = 0; i < stateDirectories.size(); i++) {
            try {
                stateDirectories.get(i).v2().syncMetaData();
            } catch (IOException e) {
                throw new WriteStateException(true, "meta data directory fsync has failed " + stateDirectories.get(i).v1(), e);
            }
        }
    }

    /**
     * Writes the given state to the given directories and performs cleanup of old state files if the write succeeds or
     * newly created state file if write fails.
     * See also {@link #write(Object, Path)} and {@link #cleanupOldFiles(long, Path)}.
     */
    public final long writeAndCleanup(final T state, final Path location) throws WriteStateException {
        return write(state, true, location);
    }

    /**
     * Writes the given state to the given directories. The state is written to a
     * state directory ({@value #STATE_DIR_NAME}) underneath each of the given file locations and is created if it
     * doesn't exist. The state is serialized to a temporary file in that directory and is then atomically moved to
     * it's target filename of the pattern {@code {prefix}{version}.st}.
     * If this method returns without exception there is a guarantee that state is persisted to the disk and loadLatestState will return
     * it.<br>
     * This method always performs cleanup of temporary files regardless whether it succeeds or fails. Cleanup logic for state files is
     * more involved.
     * If this method fails with an exception, it performs cleanup of newly created state file.
     * But if this method succeeds, it does not perform cleanup of old state files.
     * If this write succeeds, but some further write fails, you may want to rollback the transaction and keep old file around.
     * After transaction is finished use {@link #cleanupOldFiles(long, Path)} for the clean-up.
     * If this write is not a part of bigger transaction, consider using {@link #writeAndCleanup(Object, Path)} method instead.
     *
     * @param state     the state object to write
     * @param location  the data dir the state should be written into
     * @throws WriteStateException if some exception during writing state occurs. See also {@link WriteStateException#isDirty()}.
     * @return generation of newly written state.
     */
    public final long write(final T state, final Path location) throws WriteStateException {
        return write(state, false, location);
    }

    private long write(final T state, boolean cleanup, final Path location) throws WriteStateException {
        if (location == null) {
            throw new IllegalArgumentException("Locations must not be null");
        }

        final long oldGenerationId, newGenerationId;
        try {
            oldGenerationId = findMaxGenerationId(prefix, location);
            newGenerationId = oldGenerationId + 1;
        } catch (Exception e) {
            throw new WriteStateException(false, "exception during looking up new generation id", e);
        }
        assert newGenerationId >= 0 : "newGenerationId must be positive but was: [" + oldGenerationId + "]";

        final String fileName = getStateFileName(newGenerationId);
        final String tmpFileName = fileName + ".tmp";
        List<Tuple<Path, Directory>> directories = new ArrayList<>();

        try {
            Path stateLocation = location.resolve(STATE_DIR_NAME);
            try {
                directories.add(new Tuple<>(location, newDirectory(stateLocation)));
            } catch (IOException e) {
                throw new WriteStateException(false, "failed to open state directory " + stateLocation, e);
            }

            writeStateToFirstLocation(state, directories.get(0).v1(), directories.get(0).v2(), tmpFileName);
            copyStateToExtraLocations(directories, tmpFileName);
            performRenames(tmpFileName, fileName, directories);
            performStateDirectoriesFsync(directories);
        } catch (WriteStateException e) {
            if (cleanup) {
                cleanupOldFiles(oldGenerationId, location);
            }
            throw e;
        } finally {
            for (Tuple<Path, Directory> pathAndDirectory : directories) {
                deleteFileIgnoreExceptions(pathAndDirectory.v1(), pathAndDirectory.v2(), tmpFileName);
                IOUtils.closeWhileHandlingException(pathAndDirectory.v2());
            }
        }

        if (cleanup) {
            cleanupOldFiles(newGenerationId, location);
        }

        return newGenerationId;
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
            try (IndexInput indexInput = EndiannessReverserUtil.openInput(dir, file.getFileName().toString(), IOContext.DEFAULT)) {
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
        return new NIOFSDirectory(dir);
    }


    /**
     * Clean ups all state files not matching passed generation.
     *
     * @param currentGeneration state generation to keep.
     * @param location          data dir.
     */
    public void cleanupOldFiles(final long currentGeneration, Path location) {
        final String fileNameToKeep = getStateFileName(currentGeneration);
        logger.trace("cleanupOldFiles: cleaning up {}", location);
        Path stateLocation = location.resolve(STATE_DIR_NAME);
        try (Directory stateDir = newDirectory(stateLocation)) {
            for (String file : stateDir.listAll()) {
                if (file.startsWith(prefix) && file.equals(fileNameToKeep) == false) {
                    deleteFileIgnoreExceptions(stateLocation, stateDir, file);
                }
            }
        } catch (Exception e) {
            logger.trace("clean up failed for state location {}", stateLocation);
        }
    }

    /**
     * Finds state file with maximum id.
     *
     * @param prefix    - filename prefix
     * @param dataLocation - path to directory with state folder
     * @return maximum id of state file or -1 if no such files are found
     * @throws IOException if IOException occurs
     */
    private long findMaxGenerationId(final String prefix, Path dataLocation) throws IOException {
        long maxId = -1;
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
        return maxId;
    }

    private Path findStateFilesByGeneration(final long generation, Path dataLocation) {
        if (generation == -1) {
            return null;
        }

        final String fileName = getStateFileName(generation);
        final Path stateFilePath = dataLocation.resolve(STATE_DIR_NAME).resolve(fileName);
        if (Files.exists(stateFilePath)) {
            logger.trace("found state file: {}", stateFilePath);
            return stateFilePath;
        }

        return null;
    }

    public String getStateFileName(long generation) {
        return prefix + generation + STATE_FILE_EXTENSION;
    }

    /**
     * Tries to load the state of particular generation from the given data-locations. If any of data locations contain state files with
     * given generation, state will be loaded from these state files.
     *
     * @param logger a logger instance.
     * @param generation the generation to be loaded.
     * @param dataLocation the data dir to read from
     * @return the state of asked generation or <code>null</code> if no state was found.
     */
    public T loadGeneration(Logger logger, NamedXContentRegistry namedXContentRegistry, long generation, Path dataLocation) {
        Path stateFile = findStateFilesByGeneration(generation, dataLocation);

        if (stateFile != null) {
            try {
                T state = read(namedXContentRegistry, stateFile);
                logger.trace("generation id [{}] read from [{}]", generation, stateFile.getFileName());
                return state;
            } catch (Exception e) {
                logger.debug(() -> new ParameterizedMessage("{}: failed to read [{}], ignoring...", stateFile, prefix), e);
                throw new ElasticsearchException("failed to read " + stateFile, e);
            }
        }
        return null;
    }

    /**
     * Tries to load the latest state from the given data-locations.
     *
     * @param logger        a logger instance.
     * @param dataLocation the data dir to read from
     * @return tuple of the latest state and generation. (null, -1) if no state is found.
     */
    public Tuple<T, Long> loadLatestStateWithGeneration(Logger logger, NamedXContentRegistry namedXContentRegistry, Path dataLocation)
            throws IOException {
        long generation = findMaxGenerationId(prefix, dataLocation);
        T state = loadGeneration(logger, namedXContentRegistry, generation, dataLocation);

        if (generation > -1 && state == null) {
            throw new IllegalStateException("unable to find state files with generation id " + generation +
                    " returned by findMaxGenerationId function, in data folder [" + dataLocation + "], concurrent writes?");
        }
        return Tuple.tuple(state, generation);
    }

    /**
     * Tries to load the latest state from the given data-locations.
     *
     * @param logger        a logger instance.
     * @param dataLocation the data dir to read from
     * @return the latest state or <code>null</code> if no state was found.
     */
    public T loadLatestState(Logger logger, NamedXContentRegistry namedXContentRegistry, Path dataLocation) throws IOException {
        return loadLatestStateWithGeneration(logger, namedXContentRegistry, dataLocation).v1();
    }

    /**
     * Deletes all meta state directories recursively for the given data locations
     * @param dataLocation the data dir to delete state from
     */
    public static void deleteMetaState(Path dataLocation) throws IOException {
        IOUtils.rm(dataLocation.resolve(STATE_DIR_NAME));
    }

    public String getPrefix() {
        return prefix;
    }
}
