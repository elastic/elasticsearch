/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.SuppressForbidden;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Clone of Lucene's SegmentInfos class that allows parsing older formats
 */
@SuppressWarnings("CheckStyle")
@SuppressForbidden(reason = "Lucene class")
public class OldSegmentInfos implements Iterable<SegmentCommitInfo> {

    /**
     * The minimum supported major version that can be opened
     */
    private static final int MIN_SUPPORTED_MAJOR_VERSION = 6;

    /**
     * Adds the {@link Version} that committed this segments_N file, as well as the {@link Version}
     * of the oldest segment, since 5.3+
     */
    private static final int VERSION_53 = 6;
    /**
     * The version that added information about the Lucene version at the time when the index has been
     * created.
     */
    private static final int VERSION_70 = 7;
    /** The version that updated segment name counter to be long instead of int. */
    private static final int VERSION_72 = 8;
    /** The version that recorded softDelCount */
    private static final int VERSION_74 = 9;
    /** The version that recorded SegmentCommitInfo IDs */
    private static final int VERSION_86 = 10;

    private static final int VERSION_CURRENT = VERSION_86;

    /** Name of the generation reference file name */
    private static final String OLD_SEGMENTS_GEN = "segments.gen";

    /** Used to name new segments. */
    public long counter;

    /** Counts how often the index has been changed. */
    public long version;

    private long generation; // generation of the "segments_N" we last successfully read

    /** Opaque Map&lt;String, String&gt; that user can specify during IndexWriter.commit */
    public Map<String, String> userData = Collections.emptyMap();

    private final List<SegmentCommitInfo> segments = new ArrayList<>();

    /** Which Lucene version wrote this commit. */
    private Version luceneVersion;

    /** Version of the oldest segment in the index, or null if there are no segments. */
    private Version minSegmentLuceneVersion;

    /** The Lucene version major that was used to create the index. */
    private final int indexCreatedVersionMajor;

    /**
     * Sole constructor.
     *
     * @param indexCreatedVersionMajor the Lucene version major at index creation time, or 6 if the
     *     index was created before 7.0
     */
    public OldSegmentInfos(int indexCreatedVersionMajor) {
        if (indexCreatedVersionMajor > Version.LATEST.major) {
            throw new IllegalArgumentException("indexCreatedVersionMajor is in the future: " + indexCreatedVersionMajor);
        }
        if (indexCreatedVersionMajor < MIN_SUPPORTED_MAJOR_VERSION) {
            throw new IllegalArgumentException(
                "indexCreatedVersionMajor must be >= " + MIN_SUPPORTED_MAJOR_VERSION + ", got: " + indexCreatedVersionMajor
            );
        }
        this.indexCreatedVersionMajor = indexCreatedVersionMajor;
    }

    /** Returns {@link SegmentCommitInfo} at the provided index. */
    public SegmentCommitInfo info(int i) {
        return segments.get(i);
    }

    /**
     * Get the generation of the most recent commit to the list of index files (N in the segments_N
     * file).
     *
     * @param files -- array of file names to check
     */
    public static long getLastCommitGeneration(String[] files) {
        long max = -1;
        for (String file : files) {
            if (file.startsWith(IndexFileNames.SEGMENTS) &&
            // skipping this file here helps deliver the right exception when opening an old index
                file.startsWith(OLD_SEGMENTS_GEN) == false) {
                long gen = generationFromSegmentsFileName(file);
                if (gen > max) {
                    max = gen;
                }
            }
        }
        return max;
    }

    /** Get the segments_N filename in use by this segment infos. */
    public String getSegmentsFileName() {
        return IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", generation);
    }

    /** Parse the generation off the segments file name and return it. */
    public static long generationFromSegmentsFileName(String fileName) {
        if (fileName.equals(OLD_SEGMENTS_GEN)) {
            throw new IllegalArgumentException("\"" + OLD_SEGMENTS_GEN + "\" is not a valid segment file name since 4.0");
        } else if (fileName.equals(IndexFileNames.SEGMENTS)) {
            return 0;
        } else if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
            return Long.parseLong(fileName.substring(1 + IndexFileNames.SEGMENTS.length()), Character.MAX_RADIX);
        } else {
            throw new IllegalArgumentException("fileName \"" + fileName + "\" is not a segments file");
        }
    }

    static OldSegmentInfos readCommit(Directory directory, String segmentFileName, int minSupportedMajorVersion) throws IOException {

        long generation = generationFromSegmentsFileName(segmentFileName);
        // System.out.println(Thread.currentThread() + ": SegmentInfos.readCommit " + segmentFileName);
        try (ChecksumIndexInput input = directory.openChecksumInput(segmentFileName)) {
            try {
                return readCommit(directory, input, generation, minSupportedMajorVersion);
            } catch (EOFException | NoSuchFileException | FileNotFoundException e) {
                throw new CorruptIndexException("Unexpected file read error while reading index.", input, e);
            }
        }
    }

    /** Read the commit from the provided {@link ChecksumIndexInput}. */
    static OldSegmentInfos readCommit(Directory directory, ChecksumIndexInput input, long generation, int minSupportedMajorVersion)
        throws IOException {
        Throwable priorE = null;
        int format = -1;
        try {
            // NOTE: as long as we want to throw indexformattooold (vs corruptindexexception), we need
            // to read the magic ourselves.
            int magic = CodecUtil.readBEInt(input);
            if (magic != CodecUtil.CODEC_MAGIC) {
                throw new IndexFormatTooOldException(input, magic, CodecUtil.CODEC_MAGIC, CodecUtil.CODEC_MAGIC);
            }
            format = CodecUtil.checkHeaderNoMagic(input, "segments", VERSION_53, VERSION_CURRENT);
            byte[] id = new byte[StringHelper.ID_LENGTH];
            input.readBytes(id, 0, id.length);
            CodecUtil.checkIndexHeaderSuffix(input, Long.toString(generation, Character.MAX_RADIX));

            Version luceneVersion = Version.fromBits(input.readVInt(), input.readVInt(), input.readVInt());
            int indexCreatedVersion = minSupportedMajorVersion;
            if (format >= VERSION_70) {
                indexCreatedVersion = input.readVInt();
            }
            if (luceneVersion.major < indexCreatedVersion) {
                throw new CorruptIndexException(
                    "Creation version ["
                        + indexCreatedVersion
                        + ".x] can't be greater than the version that wrote the segment infos: ["
                        + luceneVersion
                        + "]",
                    input
                );
            }

            if (indexCreatedVersion < minSupportedMajorVersion) {
                throw new IndexFormatTooOldException(
                    input,
                    "This index was initially created with Lucene "
                        + indexCreatedVersion
                        + ".x while the current version is "
                        + Version.LATEST
                        + " and Lucene only supports reading"
                        + (minSupportedMajorVersion == Version.MIN_SUPPORTED_MAJOR
                            ? " the current and previous major versions"
                            : " from version " + minSupportedMajorVersion + " upwards")
                );
            }

            OldSegmentInfos infos = new OldSegmentInfos(indexCreatedVersion);
            infos.generation = generation;
            infos.luceneVersion = luceneVersion;
            parseSegmentInfos(directory, input, infos, format);
            return infos;

        } catch (Throwable t) {
            priorE = t;
        } finally {
            if (format >= VERSION_53) { // oldest supported version
                CodecUtil.checkFooter(input, priorE);
            } else {
                throw IOUtils.rethrowAlways(priorE);
            }
        }
        throw new Error("Unreachable code");
    }

    private static void parseSegmentInfos(Directory directory, DataInput input, OldSegmentInfos infos, int format) throws IOException {
        infos.version = CodecUtil.readBELong(input);
        // System.out.println("READ sis version=" + infos.version);
        if (format > VERSION_70) {
            infos.counter = input.readVLong();
        } else {
            infos.counter = CodecUtil.readBEInt(input);
        }
        int numSegments = CodecUtil.readBEInt(input);
        if (numSegments < 0) {
            throw new CorruptIndexException("invalid segment count: " + numSegments, input);
        }

        if (numSegments > 0) {
            infos.minSegmentLuceneVersion = Version.fromBits(input.readVInt(), input.readVInt(), input.readVInt());
        } else {
            // else leave as null: no segments
        }

        for (int seg = 0; seg < numSegments; seg++) {
            String segName = input.readString();
            if (format < VERSION_70) {
                byte hasID = input.readByte();
                if (hasID == 0) {
                    throw new IndexFormatTooOldException(input, "Segment is from Lucene 4.x");
                } else if (hasID != 1) {
                    throw new CorruptIndexException("invalid hasID byte, got: " + hasID, input);
                }
            }
            byte[] segmentID = new byte[StringHelper.ID_LENGTH];
            input.readBytes(segmentID, 0, segmentID.length);
            Codec codec = readCodec(input);
            SegmentInfo info = codec.segmentInfoFormat().read(directory, segName, segmentID, IOContext.DEFAULT);
            info.setCodec(codec);
            long delGen = CodecUtil.readBELong(input);
            int delCount = CodecUtil.readBEInt(input);
            if (delCount < 0 || delCount > info.maxDoc()) {
                throw new CorruptIndexException("invalid deletion count: " + delCount + " vs maxDoc=" + info.maxDoc(), input);
            }
            long fieldInfosGen = CodecUtil.readBELong(input);
            long dvGen = CodecUtil.readBELong(input);
            int softDelCount = format > VERSION_72 ? CodecUtil.readBEInt(input) : 0;
            if (softDelCount < 0 || softDelCount > info.maxDoc()) {
                throw new CorruptIndexException("invalid deletion count: " + softDelCount + " vs maxDoc=" + info.maxDoc(), input);
            }
            if (softDelCount + delCount > info.maxDoc()) {
                throw new CorruptIndexException(
                    "invalid deletion count: " + (softDelCount + delCount) + " vs maxDoc=" + info.maxDoc(),
                    input
                );
            }
            final byte[] sciId;
            if (format > VERSION_74) {
                byte marker = input.readByte();
                switch (marker) {
                    case 1 -> {
                        sciId = new byte[StringHelper.ID_LENGTH];
                        input.readBytes(sciId, 0, sciId.length);
                    }
                    case 0 -> sciId = null;
                    default -> throw new CorruptIndexException("invalid SegmentCommitInfo ID marker: " + marker, input);
                }
            } else {
                sciId = null;
            }
            SegmentCommitInfo siPerCommit = new SegmentCommitInfo(info, delCount, softDelCount, delGen, fieldInfosGen, dvGen, sciId);
            siPerCommit.setFieldInfosFiles(input.readSetOfStrings());
            final Map<Integer, Set<String>> dvUpdateFiles;
            final int numDVFields = CodecUtil.readBEInt(input);
            if (numDVFields == 0) {
                dvUpdateFiles = Collections.emptyMap();
            } else {
                Map<Integer, Set<String>> map = Maps.newMapWithExpectedSize(numDVFields);
                for (int i = 0; i < numDVFields; i++) {
                    map.put(CodecUtil.readBEInt(input), input.readSetOfStrings());
                }
                dvUpdateFiles = Collections.unmodifiableMap(map);
            }
            siPerCommit.setDocValuesUpdatesFiles(dvUpdateFiles);
            infos.add(siPerCommit);

            Version segmentVersion = info.getVersion();

            if (segmentVersion.onOrAfter(infos.minSegmentLuceneVersion) == false) {
                throw new CorruptIndexException(
                    "segments file recorded minSegmentLuceneVersion="
                        + infos.minSegmentLuceneVersion
                        + " but segment="
                        + info
                        + " has older version="
                        + segmentVersion,
                    input
                );
            }

            if (infos.indexCreatedVersionMajor >= 7 && segmentVersion.major < infos.indexCreatedVersionMajor) {
                throw new CorruptIndexException(
                    "segments file recorded indexCreatedVersionMajor="
                        + infos.indexCreatedVersionMajor
                        + " but segment="
                        + info
                        + " has older version="
                        + segmentVersion,
                    input
                );
            }

            if (infos.indexCreatedVersionMajor >= 7 && info.getMinVersion() == null) {
                throw new CorruptIndexException(
                    "segments infos must record minVersion with indexCreatedVersionMajor=" + infos.indexCreatedVersionMajor,
                    input
                );
            }
        }

        infos.userData = input.readMapOfStrings();
    }

    private static Codec readCodec(DataInput input) throws IOException {
        final String name = input.readString();
        try {
            return Codec.forName(name);
        } catch (IllegalArgumentException e) {
            // maybe it's an old default codec that moved
            if (name.startsWith("Lucene")) {
                throw new IllegalArgumentException(
                    "Could not load codec '" + name + "'. Did you forget to add lucene-backward-codecs.jar?",
                    e
                );
            }
            throw e;
        }
    }

    static OldSegmentInfos readLatestCommit(Directory directory) throws IOException {
        return new OldSegmentInfos.FindSegmentsFile<OldSegmentInfos>(directory) {
            @Override
            protected OldSegmentInfos doBody(String segmentFileName) throws IOException {
                return readCommit(directory, segmentFileName, MIN_SUPPORTED_MAJOR_VERSION);
            }
        }.run();
    }

    /** Returns current generation. */
    public long getGeneration() {
        return generation;
    }

    public Version getLuceneVersion() {
        return luceneVersion;
    }

    /**
     * Utility class for executing code that needs to do something with the current segments file.
     * This is necessary with lock-less commits because from the time you locate the current segments
     * file name, until you actually open it, read its contents, or check modified time, etc., it
     * could have been deleted due to a writer commit finishing.
     */
    @SuppressWarnings("CheckStyle")
    public abstract static class FindSegmentsFile<T> {

        final Directory directory;

        /** Sole constructor. */
        protected FindSegmentsFile(Directory directory) {
            this.directory = directory;
        }

        /** Locate the most recent {@code segments} file and run {@link #doBody} on it. */
        public T run() throws IOException {
            long lastGen;
            long gen = -1;
            IOException exc = null;

            // Loop until we succeed in calling doBody() without
            // hitting an IOException. An IOException most likely
            // means an IW deleted our commit while opening
            // the time it took us to load the now-old infos files
            // (and segments files). It's also possible it's a
            // true error (corrupt index). To distinguish these,
            // on each retry we must see "forward progress" on
            // which generation we are trying to load. If we
            // don't, then the original error is real and we throw
            // it.

            for (;;) {
                lastGen = gen;
                String[] files = directory.listAll();
                String[] files2 = directory.listAll();
                Arrays.sort(files);
                Arrays.sort(files2);
                if (Arrays.equals(files, files2) == false) {
                    // listAll() is weakly consistent, this means we hit "concurrent modification exception"
                    continue;
                }
                gen = getLastCommitGeneration(files);

                if (gen == -1) {
                    throw new IndexNotFoundException("no segments* file found in " + directory + ": files: " + Arrays.toString(files));
                } else if (gen > lastGen) {
                    String segmentFileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen);

                    try {
                        return doBody(segmentFileName);
                    } catch (IOException err) {
                        // Save the original root cause:
                        if (exc == null) {
                            exc = err;
                        }
                    }
                } else {
                    throw exc;
                }
            }
        }

        /**
         * Subclass must implement this. The assumption is an IOException will be thrown if something
         * goes wrong during the processing that could have been caused by a writer committing.
         */
        protected abstract T doBody(String segmentFileName) throws IOException;
    }

    /** Returns readable description of this segment. */
    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getSegmentsFileName()).append(": ");
        final int count = size();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                buffer.append(' ');
            }
            final SegmentCommitInfo info = info(i);
            buffer.append(info.toString(0));
        }
        return buffer.toString();
    }

    /**
     * Return {@code userData} saved with this commit.
     *
     * @see IndexWriter#commit()
     */
    public Map<String, String> getUserData() {
        return userData;
    }

    /** Returns an <b>unmodifiable</b> {@link Iterator} of contained segments in order. */
    @Override
    public Iterator<SegmentCommitInfo> iterator() {
        return asList().iterator();
    }

    /** Returns all contained segments as an <b>unmodifiable</b> {@link List} view. */
    public List<SegmentCommitInfo> asList() {
        return Collections.unmodifiableList(segments);
    }

    /** Returns number of {@link SegmentCommitInfo}s. */
    public int size() {
        return segments.size();
    }

    /** Appends the provided {@link SegmentCommitInfo}. */
    public void add(SegmentCommitInfo si) {
        segments.add(si);
    }
}
