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
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@LuceneTestCase.SuppressFileSystems("ExtrasFS") // TODO: fix test to work with ExtrasFS
public class MetaDataStateFormatTests extends ESTestCase {
    /**
     * Ensure we can read a pre-generated cluster state.
     */
    public void testReadClusterState() throws IOException {
        final MetaDataStateFormat<MetaData> format = new MetaDataStateFormat<MetaData>("global-") {

            @Override
            public void toXContent(XContentBuilder builder, MetaData state) {
                fail("this test doesn't write");
            }

            @Override
            public MetaData fromXContent(XContentParser parser) throws IOException {
                return MetaData.Builder.fromXContent(parser);
            }
        };
        Path tmp = createTempDir();
        final InputStream resource = this.getClass().getResourceAsStream("global-3.st");
        assertThat(resource, notNullValue());
        Path dst = tmp.resolve("global-3.st");
        Files.copy(resource, dst);
        MetaData read = format.read(xContentRegistry(), dst);
        assertThat(read, notNullValue());
        assertThat(read.clusterUUID(), equalTo("y9XcwLJGTROoOEfixlRwfQ"));
        // indices are empty since they are serialized separately
    }

    public void testReadWriteState() throws IOException {
        Path[] dirs = new Path[randomIntBetween(1, 5)];
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = createTempDir();
        }
        final long id = addDummyFiles("foo-", dirs);
        Format format = new Format("foo-");
        DummyState state = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 1000), randomInt(), randomLong(),
            randomDouble(), randomBoolean());
        format.writeAndCleanup(state, dirs);
        for (Path file : dirs) {
            Path[] list = content("*", file);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo(MetaDataStateFormat.STATE_DIR_NAME));
            Path stateDir = list[0];
            assertThat(Files.isDirectory(stateDir), is(true));
            list = content("foo-*", stateDir);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo("foo-" + id + ".st"));
            DummyState read = format.read(NamedXContentRegistry.EMPTY, list[0]);
            assertThat(read, equalTo(state));
        }
        DummyState state2 = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 1000), randomInt(), randomLong(),
            randomDouble(), randomBoolean());
        format.writeAndCleanup(state2, dirs);

        for (Path file : dirs) {
            Path[] list = content("*", file);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo(MetaDataStateFormat.STATE_DIR_NAME));
            Path stateDir = list[0];
            assertThat(Files.isDirectory(stateDir), is(true));
            list = content("foo-*", stateDir);
            assertEquals(list.length,1);
            assertThat(list[0].getFileName().toString(), equalTo("foo-"+ (id+1) + ".st"));
            DummyState read = format.read(NamedXContentRegistry.EMPTY, list[0]);
            assertThat(read, equalTo(state2));

        }
    }

    public void testVersionMismatch() throws IOException {
        Path[] dirs = new Path[randomIntBetween(1, 5)];
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = createTempDir();
        }
        final long id = addDummyFiles("foo-", dirs);

        Format format = new Format("foo-");
        DummyState state = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 1000), randomInt(), randomLong(),
            randomDouble(), randomBoolean());
        format.writeAndCleanup(state, dirs);
        for (Path file : dirs) {
            Path[] list = content("*", file);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo(MetaDataStateFormat.STATE_DIR_NAME));
            Path stateDir = list[0];
            assertThat(Files.isDirectory(stateDir), is(true));
            list = content("foo-*", stateDir);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo("foo-" + id + ".st"));
            DummyState read = format.read(NamedXContentRegistry.EMPTY, list[0]);
            assertThat(read, equalTo(state));
        }
    }

    public void testCorruption() throws IOException {
        Path[] dirs = new Path[randomIntBetween(1, 5)];
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = createTempDir();
        }
        final long id = addDummyFiles("foo-", dirs);
        Format format = new Format("foo-");
        DummyState state = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 1000), randomInt(), randomLong(),
            randomDouble(), randomBoolean());
        format.writeAndCleanup(state, dirs);
        for (Path file : dirs) {
            Path[] list = content("*", file);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo(MetaDataStateFormat.STATE_DIR_NAME));
            Path stateDir = list[0];
            assertThat(Files.isDirectory(stateDir), is(true));
            list = content("foo-*", stateDir);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo("foo-" + id + ".st"));
            DummyState read = format.read(NamedXContentRegistry.EMPTY, list[0]);
            assertThat(read, equalTo(state));
            // now corrupt it
            corruptFile(list[0], logger);
            try {
                format.read(NamedXContentRegistry.EMPTY, list[0]);
                fail("corrupted file");
            } catch (CorruptStateException ex) {
                // expected
            }
        }
    }

    public static void corruptFile(Path fileToCorrupt, Logger logger) throws IOException {
        try (SimpleFSDirectory dir = new SimpleFSDirectory(fileToCorrupt.getParent())) {
            long checksumBeforeCorruption;
            try (IndexInput input = dir.openInput(fileToCorrupt.getFileName().toString(), IOContext.DEFAULT)) {
                checksumBeforeCorruption = CodecUtil.retrieveChecksum(input);
            }
            try (FileChannel raf = FileChannel.open(fileToCorrupt, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                raf.position(randomIntBetween(0, (int)Math.min(Integer.MAX_VALUE, raf.size()-1)));
                long filePointer = raf.position();
                ByteBuffer bb = ByteBuffer.wrap(new byte[1]);
                raf.read(bb);

                bb.flip();
                byte oldValue = bb.get(0);
                byte newValue = (byte) ~oldValue;
                bb.put(0, newValue);
                raf.write(bb, filePointer);
                logger.debug("Corrupting file {} --  flipping at position {} from {} to {} ", fileToCorrupt.getFileName().toString(),
                    filePointer, Integer.toHexString(oldValue), Integer.toHexString(newValue));
            }
        long checksumAfterCorruption;
        long actualChecksumAfterCorruption;
        try (ChecksumIndexInput input = dir.openChecksumInput(fileToCorrupt.getFileName().toString(), IOContext.DEFAULT)) {
            assertThat(input.getFilePointer(), is(0L));
            input.seek(input.length() - 8); // one long is the checksum... 8 bytes
            checksumAfterCorruption = input.getChecksum();
            actualChecksumAfterCorruption = input.readLong();
        }
        StringBuilder msg = new StringBuilder();
        msg.append("Checksum before: [").append(checksumBeforeCorruption).append("]");
        msg.append(" after: [").append(checksumAfterCorruption).append("]");
        msg.append(" checksum value after corruption: ").append(actualChecksumAfterCorruption).append("]");
        msg.append(" file: ").append(fileToCorrupt.getFileName().toString()).append(" length: ")
            .append(dir.fileLength(fileToCorrupt.getFileName().toString()));
        logger.debug("{}", msg.toString());
        assumeTrue("Checksum collision - " + msg.toString(),
                checksumAfterCorruption != checksumBeforeCorruption // collision
                        || actualChecksumAfterCorruption != checksumBeforeCorruption); // checksum corrupted
        }
    }

    public void testLoadState() throws IOException {
        final Path[] dirs = new Path[randomIntBetween(1, 5)];
        int numStates = randomIntBetween(1, 5);
        List<MetaData> meta = new ArrayList<>();
        for (int i = 0; i < numStates; i++) {
            meta.add(randomMeta());
        }
        Set<Path> corruptedFiles = new HashSet<>();
        MetaDataStateFormat<MetaData> format = metaDataFormat();
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = createTempDir();
            Files.createDirectories(dirs[i].resolve(MetaDataStateFormat.STATE_DIR_NAME));
            for (int j = 0; j < numStates; j++) {
                format.writeAndCleanup(meta.get(j), dirs[i]);
                if (randomBoolean() && (j < numStates - 1 || dirs.length > 0 && i != 0)) {  // corrupt a file that we do not necessarily
                                                                                            // need here....
                    Path file = dirs[i].resolve(MetaDataStateFormat.STATE_DIR_NAME).resolve("global-" + j + ".st");
                    corruptedFiles.add(file);
                    MetaDataStateFormatTests.corruptFile(file, logger);
                }
            }

        }
        List<Path> dirList = Arrays.asList(dirs);
        Collections.shuffle(dirList, random());
        MetaData loadedMetaData = format.loadLatestState(logger, xContentRegistry(), dirList.toArray(new Path[0]));
        MetaData latestMetaData = meta.get(numStates-1);
        assertThat(loadedMetaData.clusterUUID(), not(equalTo("_na_")));
        assertThat(loadedMetaData.clusterUUID(), equalTo(latestMetaData.clusterUUID()));
        ImmutableOpenMap<String,IndexMetaData> indices = loadedMetaData.indices();
        assertThat(indices.size(), equalTo(latestMetaData.indices().size()));
        for (IndexMetaData original : latestMetaData) {
            IndexMetaData deserialized = indices.get(original.getIndex().getName());
            assertThat(deserialized, notNullValue());
            assertThat(deserialized.getVersion(), equalTo(original.getVersion()));
            assertThat(deserialized.getMappingVersion(), equalTo(original.getMappingVersion()));
            assertThat(deserialized.getSettingsVersion(), equalTo(original.getSettingsVersion()));
            assertThat(deserialized.getNumberOfReplicas(), equalTo(original.getNumberOfReplicas()));
            assertThat(deserialized.getNumberOfShards(), equalTo(original.getNumberOfShards()));
        }

        // make sure the index tombstones are the same too
        assertThat(loadedMetaData.indexGraveyard(), equalTo(latestMetaData.indexGraveyard()));

        // now corrupt all the latest ones and make sure we fail to load the state
        for (int i = 0; i < dirs.length; i++) {
            Path file = dirs[i].resolve(MetaDataStateFormat.STATE_DIR_NAME).resolve("global-" + (numStates-1) + ".st");
            if (corruptedFiles.contains(file)) {
                continue;
            }
            MetaDataStateFormatTests.corruptFile(file, logger);
        }
        try {
            format.loadLatestState(logger, xContentRegistry(), dirList.toArray(new Path[0]));
            fail("latest version can not be read");
        } catch (ElasticsearchException ex) {
            assertThat(ExceptionsHelper.unwrap(ex, CorruptStateException.class), notNullValue());
        }
    }

    private DummyState writeAndReadStateSuccessfully(Format format, Path... paths) throws IOException {
        format.noFailures();
        DummyState state = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 100), randomInt(), randomLong(),
                randomDouble(), randomBoolean());
        format.writeAndCleanup(state, paths);
        assertEquals(state, format.loadLatestState(logger, NamedXContentRegistry.EMPTY, paths));
        ensureOnlyOneStateFile(paths);
        return state;
    }

    private static void ensureOnlyOneStateFile(Path[] paths) throws IOException {
        for (Path path : paths) {
            try (Directory dir = new SimpleFSDirectory(path.resolve(MetaDataStateFormat.STATE_DIR_NAME))) {
                assertThat(dir.listAll().length, equalTo(1));
            }
        }
    }

    public void testFailWriteAndReadPreviousState() throws IOException {
        Path path = createTempDir();
        Format format = new Format("foo-");

        DummyState initialState = writeAndReadStateSuccessfully(format, path);

        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            format.failOnMethods(Format.FAIL_DELETE_TMP_FILE, Format.FAIL_CREATE_OUTPUT_FILE, Format.FAIL_WRITE_TO_OUTPUT_FILE,
                    Format.FAIL_FSYNC_TMP_FILE, Format.FAIL_RENAME_TMP_FILE);
            DummyState newState = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 100), randomInt(), randomLong(),
                    randomDouble(), randomBoolean());
            WriteStateException ex = expectThrows(WriteStateException.class, () -> format.writeAndCleanup(newState, path));
            assertFalse(ex.isDirty());

            format.noFailures();
            assertEquals(initialState, format.loadLatestState(logger, NamedXContentRegistry.EMPTY, path));
        }

        writeAndReadStateSuccessfully(format, path);
    }

    public void testFailWriteAndReadAnyState() throws IOException {
        Path path = createTempDir();
        Format format = new Format("foo-");
        Set<DummyState> possibleStates = new HashSet<>();

        DummyState initialState = writeAndReadStateSuccessfully(format, path);
        possibleStates.add(initialState);

        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            format.failOnMethods(Format.FAIL_FSYNC_STATE_DIRECTORY);
            DummyState newState = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 100), randomInt(), randomLong(),
                    randomDouble(), randomBoolean());
            possibleStates.add(newState);
            WriteStateException ex = expectThrows(WriteStateException.class, () -> format.writeAndCleanup(newState, path));
            assertTrue(ex.isDirty());

            format.noFailures();
            assertTrue(possibleStates.contains(format.loadLatestState(logger, NamedXContentRegistry.EMPTY, path)));
        }

        writeAndReadStateSuccessfully(format, path);
    }

    public void testFailCopyTmpFileToExtraLocation() throws IOException {
        Path paths[] = new Path[randomIntBetween(2, 5)];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = createTempDir();
        }
        Format format = new Format("foo-");

        DummyState initialState = writeAndReadStateSuccessfully(format, paths);

        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            format.failOnMethods(Format.FAIL_OPEN_STATE_FILE_WHEN_COPYING);
            DummyState newState = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 100), randomInt(), randomLong(),
                    randomDouble(), randomBoolean());
            WriteStateException ex = expectThrows(WriteStateException.class, () -> format.writeAndCleanup(newState, paths));
            assertFalse(ex.isDirty());

            format.noFailures();
            assertEquals(initialState, format.loadLatestState(logger, NamedXContentRegistry.EMPTY, paths));
        }

        writeAndReadStateSuccessfully(format, paths);
    }

    public void testFailRandomlyAndReadAnyState() throws IOException {
        Path paths[] = new Path[randomIntBetween(1, 5)];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = createTempDir();
        }
        Format format = new Format("foo-");
        Set<DummyState> possibleStates = new HashSet<>();

        DummyState initialState = writeAndReadStateSuccessfully(format, paths);
        possibleStates.add(initialState);

        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            format.failRandomly();
            DummyState newState = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 100), randomInt(), randomLong(),
                    randomDouble(), randomBoolean());
            try {
                format.writeAndCleanup(newState, paths);
                possibleStates.clear();
                possibleStates.add(newState);
            } catch (WriteStateException e) {
                if (e.isDirty()) {
                    possibleStates.add(newState);
                }
            }

            format.noFailures();
            //we call loadLatestState not on full path set, but only on random paths from this set. This is to emulate disk failures.
            Path[] randomPaths = randomSubsetOf(randomIntBetween(1, paths.length), paths).toArray(new Path[0]);
            DummyState stateOnDisk = format.loadLatestState(logger, NamedXContentRegistry.EMPTY, randomPaths);
            assertTrue(possibleStates.contains(stateOnDisk));
            if (possibleStates.size() > 1) {
                //if there was a WriteStateException we need to override current state before we continue
                newState = writeAndReadStateSuccessfully(format, paths);
                possibleStates.clear();
                possibleStates.add(newState);
            }
        }

        writeAndReadStateSuccessfully(format, paths);
    }

    private static MetaDataStateFormat<MetaData> metaDataFormat() {
        return new MetaDataStateFormat<MetaData>(MetaData.GLOBAL_STATE_FILE_PREFIX) {
            @Override
            public void toXContent(XContentBuilder builder, MetaData state) throws IOException {
                MetaData.Builder.toXContent(state, builder, ToXContent.EMPTY_PARAMS);
            }

            @Override
            public MetaData fromXContent(XContentParser parser) throws IOException {
                return MetaData.Builder.fromXContent(parser);
            }
        };
    }

    private MetaData randomMeta() throws IOException {
        int numIndices = randomIntBetween(1, 10);
        MetaData.Builder mdBuilder = MetaData.builder();
        mdBuilder.generateClusterUuidIfNeeded();
        for (int i = 0; i < numIndices; i++) {
            mdBuilder.put(indexBuilder(randomAlphaOfLength(10) + "idx-"+i));
        }
        int numDelIndices = randomIntBetween(0, 5);
        final IndexGraveyard.Builder graveyard = IndexGraveyard.builder();
        for (int i = 0; i < numDelIndices; i++) {
            graveyard.addTombstone(new Index(randomAlphaOfLength(10) + "del-idx-" + i, UUIDs.randomBase64UUID()));
        }
        mdBuilder.indexGraveyard(graveyard.build());
        return mdBuilder.build();
    }

    private IndexMetaData.Builder indexBuilder(String index) throws IOException {
        return IndexMetaData.builder(index)
                .settings(settings(Version.CURRENT).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10))
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 5)));
    }


    private static class Format extends MetaDataStateFormat<DummyState> {
        private enum FailureMode {
            NO_FAILURES,
            FAIL_ON_METHOD,
            FAIL_RANDOMLY
        }

        private FailureMode failureMode;
        private String[] failureMethods;

        static final String FAIL_CREATE_OUTPUT_FILE = "createOutput";
        static final String FAIL_WRITE_TO_OUTPUT_FILE = "writeBytes";
        static final String FAIL_FSYNC_TMP_FILE = "sync";
        static final String FAIL_RENAME_TMP_FILE = "rename";
        static final String FAIL_FSYNC_STATE_DIRECTORY = "syncMetaData";
        static final String FAIL_DELETE_TMP_FILE = "deleteFile";
        static final String FAIL_OPEN_STATE_FILE_WHEN_COPYING = "openInput";

        /**
         * Constructs a MetaDataStateFormat object for storing/retrieving DummyState.
         * By default no I/O failures are injected.
         * I/O failure behaviour can be controlled by {@link #noFailures()}, {@link #failOnMethods(String...)} and
         * {@link #failRandomly()} method calls.
         */
        Format(String prefix) {
            super(prefix);
            this.failureMode = FailureMode.NO_FAILURES;
        }

        @Override
        public void toXContent(XContentBuilder builder, DummyState state) throws IOException {
            state.toXContent(builder, null);
        }

        @Override
        public DummyState fromXContent(XContentParser parser) throws IOException {
            return new DummyState().parse(parser);
        }

        public void noFailures() {
            this.failureMode = FailureMode.NO_FAILURES;
        }

        public void failOnMethods(String... failureMethods) {
            this.failureMode = FailureMode.FAIL_ON_METHOD;
            this.failureMethods = failureMethods;
        }

        public void failRandomly() {
            this.failureMode = FailureMode.FAIL_RANDOMLY;
        }

        @Override
        protected Directory newDirectory(Path dir) {
            MockDirectoryWrapper mock = newMockFSDirectory(dir);
            if (failureMode == FailureMode.FAIL_ON_METHOD) {
                final String failMethod = randomFrom(failureMethods);
                MockDirectoryWrapper.Failure fail = new MockDirectoryWrapper.Failure() {
                    @Override
                    public void eval(MockDirectoryWrapper dir) throws IOException {
                        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
                            if (failMethod.equals(e.getMethodName())) {
                                throw new MockDirectoryWrapper.FakeIOException();
                            }
                        }
                    }
                };
                mock.failOn(fail);
            } else if (failureMode == FailureMode.FAIL_RANDOMLY) {
                MockDirectoryWrapper.Failure fail = new MockDirectoryWrapper.Failure() {
                    @Override
                    public void eval(MockDirectoryWrapper dir) throws IOException {
                        if (randomIntBetween(0, 20) == 0) {
                            throw new MockDirectoryWrapper.FakeIOException();
                        }
                    }
                };
                mock.failOn(fail);
            }
            closeAfterSuite(mock);
            return mock;
        }
    }

    private static class DummyState implements ToXContentFragment {
        String string;
        int aInt;
        long aLong;
        double aDouble;
        boolean aBoolean;

        @Override
        public String toString() {
            return "DummyState{" +
                    "string='" + string + '\'' +
                    ", aInt=" + aInt +
                    ", aLong=" + aLong +
                    ", aDouble=" + aDouble +
                    ", aBoolean=" + aBoolean +
                    '}';
        }

        DummyState(String string, int aInt, long aLong, double aDouble, boolean aBoolean) {
            this.string = string;
            this.aInt = aInt;
            this.aLong = aLong;
            this.aDouble = aDouble;
            this.aBoolean = aBoolean;
        }

        DummyState() {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("string", string);
            builder.field("int", aInt);
            builder.field("long", aLong);
            builder.field("double", aDouble);
            builder.field("boolean", aBoolean);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DummyState that = (DummyState) o;

            if (aBoolean != that.aBoolean) return false;
            if (Double.compare(that.aDouble, aDouble) != 0) return false;
            if (aInt != that.aInt) return false;
            if (aLong != that.aLong) return false;
            return string.equals(that.string);

        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = string.hashCode();
            result = 31 * result + aInt;
            result = 31 * result + Long.hashCode(aLong);
            temp = Double.doubleToLongBits(aDouble);
            result = 31 * result + Long.hashCode(temp);
            result = 31 * result + (aBoolean ? 1 : 0);
            return result;
        }

        public DummyState parse(XContentParser parser) throws IOException {
            String fieldName = null;
            parser.nextToken();  // start object
            while(parser.nextToken() != XContentParser.Token.END_OBJECT) {
                XContentParser.Token token = parser.currentToken();
                if (token == XContentParser.Token.FIELD_NAME) {
                  fieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    assertTrue("string".equals(fieldName));
                    string = parser.text();
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    switch (fieldName) {
                        case "double":
                            aDouble = parser.doubleValue();
                            break;
                        case "int":
                            aInt = parser.intValue();
                            break;
                        case "long":
                            aLong = parser.longValue();
                            break;
                        default:
                            fail("unexpected numeric value " + token);
                            break;
                    }
                }else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    assertTrue("boolean".equals(fieldName));
                    aBoolean = parser.booleanValue();
                } else {
                    fail("unexpected value " + token);
                }
            }
            return this;
        }
    }

    public Path[] content(String glob, Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
            return StreamSupport.stream(stream.spliterator(), false).toArray(length -> new Path[length]);
        }
    }

    public long addDummyFiles(String prefix, Path... paths) throws IOException {
        int realId = -1;
        for (Path path : paths) {
            if (randomBoolean()) {
                Path stateDir = path.resolve(MetaDataStateFormat.STATE_DIR_NAME);
                Files.createDirectories(stateDir);
                String actualPrefix = prefix;
                int id = randomIntBetween(0, 10);
                if (randomBoolean()) {
                    actualPrefix = "dummy-";
                } else {
                   realId = Math.max(realId, id);
                }
                try (OutputStream stream =
                         Files.newOutputStream(stateDir.resolve(actualPrefix + id + MetaDataStateFormat.STATE_FILE_EXTENSION))) {
                    stream.write(0);
                }
            }
        }
        return realId + 1;
    }
}
