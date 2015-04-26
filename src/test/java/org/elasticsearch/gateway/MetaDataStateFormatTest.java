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

import com.google.common.collect.Iterators;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleMarkFailure;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems("ExtrasFS") // TODO: fix test to work with ExtrasFS
public class MetaDataStateFormatTest extends ElasticsearchTestCase {


    /**
     * Ensure we can read a pre-generated cluster state.
     */
    public void testReadClusterState() throws URISyntaxException, IOException {
        final MetaDataStateFormat<MetaData> format = new MetaDataStateFormat<MetaData>(randomFrom(XContentType.values()), "global-") {

            @Override
            public void toXContent(XContentBuilder builder, MetaData state) throws IOException {
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
        MetaData read = format.read(dst);
        assertThat(read, notNullValue());
        assertThat(read.uuid(), equalTo("3O1tDF1IRB6fSJ-GrTMUtg"));
        // indices are empty since they are serialized separately
    }

    public void testReadWriteState() throws IOException {
        Path[] dirs = new Path[randomIntBetween(1, 5)];
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = createTempDir();
        }
        final long id = addDummyFiles("foo-", dirs);
        Format format = new Format(randomFrom(XContentType.values()), "foo-");
        DummyState state = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 1000), randomInt(), randomLong(), randomDouble(), randomBoolean());
        int version = between(0, Integer.MAX_VALUE/2);
        format.write(state, version, dirs);
        for (Path file : dirs) {
            Path[] list = content("*", file);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo(MetaDataStateFormat.STATE_DIR_NAME));
            Path stateDir = list[0];
            assertThat(Files.isDirectory(stateDir), is(true));
            list = content("foo-*", stateDir);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo("foo-" + id + ".st"));
            DummyState read = format.read(list[0]);
            assertThat(read, equalTo(state));
        }
        final int version2 = between(version, Integer.MAX_VALUE);
        DummyState state2 = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 1000), randomInt(), randomLong(), randomDouble(), randomBoolean());
        format.write(state2, version2, dirs);

        for (Path file : dirs) {
            Path[] list = content("*", file);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo(MetaDataStateFormat.STATE_DIR_NAME));
            Path stateDir = list[0];
            assertThat(Files.isDirectory(stateDir), is(true));
            list = content("foo-*", stateDir);
            assertEquals(list.length,1);
            assertThat(list[0].getFileName().toString(), equalTo("foo-"+ (id+1) + ".st"));
            DummyState read = format.read(list[0]);
            assertThat(read, equalTo(state2));

        }
    }

    @Test
    public void testVersionMismatch() throws IOException {
        Path[] dirs = new Path[randomIntBetween(1, 5)];
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = createTempDir();
        }
        final long id = addDummyFiles("foo-", dirs);

        Format format = new Format(randomFrom(XContentType.values()), "foo-");
        DummyState state = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 1000), randomInt(), randomLong(), randomDouble(), randomBoolean());
        int version = between(0, Integer.MAX_VALUE/2);
        format.write(state, version, dirs);
        for (Path file : dirs) {
            Path[] list = content("*", file);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo(MetaDataStateFormat.STATE_DIR_NAME));
            Path stateDir = list[0];
            assertThat(Files.isDirectory(stateDir), is(true));
            list = content("foo-*", stateDir);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo("foo-" + id + ".st"));
            DummyState read = format.read(list[0]);
            assertThat(read, equalTo(state));
        }
    }

    public void testCorruption() throws IOException {
        Path[] dirs = new Path[randomIntBetween(1, 5)];
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = createTempDir();
        }
        final long id = addDummyFiles("foo-", dirs);
        Format format = new Format(randomFrom(XContentType.values()), "foo-");
        DummyState state = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 1000), randomInt(), randomLong(), randomDouble(), randomBoolean());
        int version = between(0, Integer.MAX_VALUE/2);
        format.write(state, version, dirs);
        for (Path file : dirs) {
            Path[] list = content("*", file);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo(MetaDataStateFormat.STATE_DIR_NAME));
            Path stateDir = list[0];
            assertThat(Files.isDirectory(stateDir), is(true));
            list = content("foo-*", stateDir);
            assertEquals(list.length, 1);
            assertThat(list[0].getFileName().toString(), equalTo("foo-" + id + ".st"));
            DummyState read = format.read(list[0]);
            assertThat(read, equalTo(state));
            // now corrupt it
            corruptFile(list[0], logger);
            try {
                format.read(list[0]);
                fail("corrupted file");
            } catch (CorruptStateException ex) {
                // expected
            }
        }
    }

    public static void corruptFile(Path file, ESLogger logger) throws IOException {
        Path fileToCorrupt = file;
        try (final SimpleFSDirectory dir = new SimpleFSDirectory(fileToCorrupt.getParent())) {
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
                logger.debug("Corrupting file {} --  flipping at position {} from {} to {} ", fileToCorrupt.getFileName().toString(), filePointer, Integer.toHexString(oldValue), Integer.toHexString(newValue));
            }
        long checksumAfterCorruption;
        long actualChecksumAfterCorruption;
        try (ChecksumIndexInput input = dir.openChecksumInput(fileToCorrupt.getFileName().toString(), IOContext.DEFAULT)) {
            assertThat(input.getFilePointer(), is(0l));
            input.seek(input.length() - 8); // one long is the checksum... 8 bytes
            checksumAfterCorruption = input.getChecksum();
            actualChecksumAfterCorruption = input.readLong();
        }
        StringBuilder msg = new StringBuilder();
        msg.append("Checksum before: [").append(checksumBeforeCorruption).append("]");
        msg.append(" after: [").append(checksumAfterCorruption).append("]");
        msg.append(" checksum value after corruption: ").append(actualChecksumAfterCorruption).append("]");
        msg.append(" file: ").append(fileToCorrupt.getFileName().toString()).append(" length: ").append(dir.fileLength(fileToCorrupt.getFileName().toString()));
        logger.debug(msg.toString());
        assumeTrue("Checksum collision - " + msg.toString(),
                checksumAfterCorruption != checksumBeforeCorruption // collision
                        || actualChecksumAfterCorruption != checksumBeforeCorruption); // checksum corrupted
        }
    }

    // If the latest version doesn't use the legacy format while previous versions do, then fail hard
    public void testLatestVersionDoesNotUseLegacy() throws IOException {
        final ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        MetaDataStateFormat<MetaData> format = MetaStateService.globalStateFormat(randomFrom(XContentType.values()), params);
        final Path[] dirs = new Path[2];
        dirs[0] = createTempDir();
        dirs[1] = createTempDir();
        for (Path dir : dirs) {
            Files.createDirectories(dir.resolve(MetaDataStateFormat.STATE_DIR_NAME));
        }
        final Path dir1 = randomFrom(dirs);
        final int v1 = randomInt(10);
        // write a first state file in the new format
        format.write(randomMeta(), v1, dir1);

        // write older state files in the old format but with a newer version
        final int numLegacyFiles = randomIntBetween(1, 5);
        for (int i = 0; i < numLegacyFiles; ++i) {
            final Path dir2 = randomFrom(dirs);
            final int v2 = v1 + 1 + randomInt(10);
            try (XContentBuilder xcontentBuilder = XContentFactory.contentBuilder(format.format(), Files.newOutputStream(dir2.resolve(MetaDataStateFormat.STATE_DIR_NAME).resolve(MetaStateService.GLOBAL_STATE_FILE_PREFIX + v2)))) {
                xcontentBuilder.startObject();
                MetaData.Builder.toXContent(randomMeta(), xcontentBuilder, params);
                xcontentBuilder.endObject();
            }
        }

        try {
            format.loadLatestState(logger, dirs);
            fail("latest version can not be read");
        } catch (ElasticsearchIllegalStateException ex) {
            assertThat(ex.getMessage(), startsWith("Could not find a state file to recover from among "));
        }
        // write the next state file in the new format and ensure it get's a higher ID
        final MetaData meta = randomMeta();
        format.write(meta, v1, dirs);
        final MetaData metaData = format.loadLatestState(logger, dirs);
        assertEquals(meta.uuid(), metaData.uuid());
        final Path path = randomFrom(dirs);
        final Path[] files = FileSystemUtils.files(path.resolve("_state"));
        assertEquals(1, files.length);
        assertEquals("global-" + format.findMaxStateId("global-", dirs) + ".st", files[0].getFileName().toString());

    }

    // If both the legacy and the new format are available for the latest version, prefer the new format
    public void testPrefersNewerFormat() throws IOException {
        final ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        MetaDataStateFormat<MetaData> format = MetaStateService.globalStateFormat(randomFrom(XContentType.values()), params);
        final Path[] dirs = new Path[2];
        dirs[0] = createTempDir();
        dirs[1] = createTempDir();
        for (Path dir : dirs) {
            Files.createDirectories(dir.resolve(MetaDataStateFormat.STATE_DIR_NAME));
        }
        final long v = randomInt(10);

        MetaData meta = randomMeta();
        String uuid = meta.uuid();

        // write a first state file in the old format
        final Path dir2 = randomFrom(dirs);
        MetaData meta2 = randomMeta();
        assertFalse(meta2.uuid().equals(uuid));
        try (XContentBuilder xcontentBuilder = XContentFactory.contentBuilder(format.format(), Files.newOutputStream(dir2.resolve(MetaDataStateFormat.STATE_DIR_NAME).resolve(MetaStateService.GLOBAL_STATE_FILE_PREFIX + v)))) {
            xcontentBuilder.startObject();
            MetaData.Builder.toXContent(randomMeta(), xcontentBuilder, params);
            xcontentBuilder.endObject();
        }

        // write a second state file in the new format but with the same version
        format.write(meta, v, dirs);

        MetaData state = format.loadLatestState(logger, dirs);
        final Path path = randomFrom(dirs);
        assertTrue(Files.exists(path.resolve(MetaDataStateFormat.STATE_DIR_NAME).resolve("global-" + (v+1) + ".st")));
        assertEquals(state.uuid(), uuid);
    }

    @Test
    public void testLoadState() throws IOException {
        final ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        final Path[] dirs = new Path[randomIntBetween(1, 5)];
        int numStates = randomIntBetween(1, 5);
        int numLegacy = randomIntBetween(0, numStates);
        List<MetaData> meta = new ArrayList<>();
        for (int i = 0; i < numStates; i++) {
            meta.add(randomMeta());
        }
        Set<Path> corruptedFiles = new HashSet<>();
        MetaDataStateFormat<MetaData> format = MetaStateService.globalStateFormat(randomFrom(XContentType.values()), params);
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = createTempDir();
            Files.createDirectories(dirs[i].resolve(MetaDataStateFormat.STATE_DIR_NAME));
            for (int j = 0; j < numLegacy; j++) {
                XContentType type = format.format();
                if (randomBoolean() && (j < numStates - 1 || dirs.length > 0 && i != 0)) {
                    Path file = dirs[i].resolve(MetaDataStateFormat.STATE_DIR_NAME).resolve("global-"+j);
                    Files.createFile(file); // randomly create 0-byte files -- there is extra logic to skip them
                } else {
                    try (XContentBuilder xcontentBuilder = XContentFactory.contentBuilder(type, Files.newOutputStream(dirs[i].resolve(MetaDataStateFormat.STATE_DIR_NAME).resolve("global-" + j)))) {
                        xcontentBuilder.startObject();
                        MetaData.Builder.toXContent(meta.get(j), xcontentBuilder, params);
                        xcontentBuilder.endObject();
                    }
                }
            }
            for (int j = numLegacy; j < numStates; j++) {
                format.write(meta.get(j), j, dirs[i]);
                if (randomBoolean() && (j < numStates - 1 || dirs.length > 0 && i != 0)) {  // corrupt a file that we do not necessarily need here....
                    Path file = dirs[i].resolve(MetaDataStateFormat.STATE_DIR_NAME).resolve("global-" + j + ".st");
                    corruptedFiles.add(file);
                    MetaDataStateFormatTest.corruptFile(file, logger);
                }
            }

        }
        List<Path> dirList = Arrays.asList(dirs);
        Collections.shuffle(dirList, getRandom());
        MetaData loadedMetaData = format.loadLatestState(logger, dirList.toArray(new Path[0]));
        MetaData latestMetaData = meta.get(numStates-1);
        assertThat(loadedMetaData.uuid(), not(equalTo("_na_")));
        assertThat(loadedMetaData.uuid(), equalTo(latestMetaData.uuid()));
        ImmutableOpenMap<String,IndexMetaData> indices = loadedMetaData.indices();
        assertThat(indices.size(), equalTo(latestMetaData.indices().size()));
        for (IndexMetaData original : latestMetaData) {
            IndexMetaData deserialized = indices.get(original.getIndex());
            assertThat(deserialized, notNullValue());
            assertThat(deserialized.version(), equalTo(original.version()));
            assertThat(deserialized.numberOfReplicas(), equalTo(original.numberOfReplicas()));
            assertThat(deserialized.numberOfShards(), equalTo(original.numberOfShards()));
        }

        // now corrupt all the latest ones and make sure we fail to load the state
        if (numStates > numLegacy) {
            for (int i = 0; i < dirs.length; i++) {
                Path file = dirs[i].resolve(MetaDataStateFormat.STATE_DIR_NAME).resolve("global-" + (numStates-1) + ".st");
                if (corruptedFiles.contains(file)) {
                    continue;
                }
                MetaDataStateFormatTest.corruptFile(file, logger);
            }
            try {
                format.loadLatestState(logger, dirList.toArray(new Path[0]));
                fail("latest version can not be read");
            } catch (ElasticsearchException ex) {
                assertThat(ex.getCause(), instanceOf(CorruptStateException.class));
            }
        }

    }

    private MetaData randomMeta() throws IOException {
        int numIndices = randomIntBetween(1, 10);
        MetaData.Builder mdBuilder = MetaData.builder();
        mdBuilder.generateUuidIfNeeded();
        for (int i = 0; i < numIndices; i++) {
            mdBuilder.put(indexBuilder(randomAsciiOfLength(10) + "idx-"+i));
        }
        return mdBuilder.build();
    }

    private IndexMetaData.Builder indexBuilder(String index) throws IOException {
        return IndexMetaData.builder(index)
                .settings(settings(Version.CURRENT).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10)).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 5)));
    }


    private class Format extends MetaDataStateFormat<DummyState> {

        Format(XContentType format, String prefix) {
            super(format, prefix);
        }

        @Override
        public void toXContent(XContentBuilder builder, DummyState state) throws IOException {
            state.toXContent(builder, null);
        }

        @Override
        public DummyState fromXContent(XContentParser parser) throws IOException {
            return new DummyState().parse(parser);
        }

        @Override
        protected Directory newDirectory(Path dir) throws IOException {
            MockDirectoryWrapper  mock = new MockDirectoryWrapper(getRandom(), super.newDirectory(dir));
            closeAfterSuite(mock);
            return mock;
        }
    }

    private static class DummyState implements ToXContent {
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

        public DummyState(String string, int aInt, long aLong, double aDouble, boolean aBoolean) {
            this.string = string;
            this.aInt = aInt;
            this.aLong = aLong;
            this.aDouble = aDouble;
            this.aBoolean = aBoolean;
        }

        public DummyState() {

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
            result = 31 * result + (int) (aLong ^ (aLong >>> 32));
            temp = Double.doubleToLongBits(aDouble);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
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

    // copied from lucene - it's package private
    final class CloseableDirectory implements Closeable {
        private final BaseDirectoryWrapper dir;
        private final TestRuleMarkFailure failureMarker;

        public CloseableDirectory(BaseDirectoryWrapper dir,
                                  TestRuleMarkFailure failureMarker) {
            this.dir = dir;
            this.failureMarker = failureMarker;
        }

        @Override
        public void close() throws IOException {
            // We only attempt to check open/closed state if there were no other test
            // failures.
            try {
                if (failureMarker.wasSuccessful() && dir.isOpen()) {
                    Assert.fail("Directory not closed: " + dir);
                }
            } finally {
                if (dir.isOpen()) {
                    dir.close();
                }
            }
        }
    }

    public Path[] content(String glob, Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
            return Iterators.toArray(stream.iterator(), Path.class);
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
                try (OutputStream stream = Files.newOutputStream(stateDir.resolve(actualPrefix + id + MetaDataStateFormat.STATE_FILE_EXTENSION))) {
                    stream.write(0);
                }
            }
        }
        return realId + 1;
    }
}
