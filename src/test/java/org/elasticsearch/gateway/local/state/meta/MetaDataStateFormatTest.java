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

import com.carrotsearch.randomizedtesting.LifecycleScope;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.*;
import org.apache.lucene.util.TestRuleMarkFailure;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.util.*;

import static org.hamcrest.Matchers.*;

public class MetaDataStateFormatTest extends ElasticsearchTestCase {


    /**
     * Ensure we can read a pre-generated cluster state.
     */
    public void testReadClusterState() throws URISyntaxException, IOException {
        final MetaDataStateFormat<MetaData> format = new MetaDataStateFormat<MetaData>(randomFrom(XContentType.values()), false) {

            @Override
            public void toXContent(XContentBuilder builder, MetaData state) throws IOException {
                fail("this test doesn't write");
            }

            @Override
            public MetaData fromXContent(XContentParser parser) throws IOException {
                return MetaData.Builder.fromXContent(parser);
            }
        };
        final URL resource = this.getClass().getResource("global-3.st");
        assertThat(resource, notNullValue());
        MetaData read = format.read(new File(resource.toURI()), 3);
        assertThat(read, notNullValue());
        assertThat(read.uuid(), equalTo("3O1tDF1IRB6fSJ-GrTMUtg"));
        // indices are empty since they are serialized separately
    }

    public void testReadWriteState() throws IOException {
        File[] dirs = new File[randomIntBetween(1, 5)];
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = newTempDir(LifecycleScope.TEST);
        }
        final boolean deleteOldFiles = randomBoolean();
        Format format = new Format(randomFrom(XContentType.values()), deleteOldFiles);
        DummyState state = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 1000), randomInt(), randomLong(), randomDouble(), randomBoolean());
        int version = between(0, Integer.MAX_VALUE/2);
        format.write(state, "foo-", version, dirs);
        for (File file : dirs) {
            File[] list = file.listFiles();
            assertEquals(list.length, 1);
            assertThat(list[0].getName(), equalTo(MetaDataStateFormat.STATE_DIR_NAME));
            File stateDir = list[0];
            assertThat(stateDir.isDirectory(), is(true));
            list = stateDir.listFiles();
            assertEquals(list.length, 1);
            assertThat(list[0].getName(), equalTo("foo-" + version + ".st"));
            DummyState read = format.read(list[0], version);
            assertThat(read, equalTo(state));
        }
        final int version2 = between(version, Integer.MAX_VALUE);
        DummyState state2 = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 1000), randomInt(), randomLong(), randomDouble(), randomBoolean());
        format.write(state2, "foo-", version2, dirs);

        for (File file : dirs) {
            File[] list = file.listFiles();
            assertEquals(list.length, 1);
            assertThat(list[0].getName(), equalTo(MetaDataStateFormat.STATE_DIR_NAME));
            File stateDir = list[0];
            assertThat(stateDir.isDirectory(), is(true));
            list = stateDir.listFiles();
            assertEquals(list.length, deleteOldFiles ? 1 : 2);
            if (deleteOldFiles) {
                assertThat(list[0].getName(), equalTo("foo-" + version2 + ".st"));
                DummyState read = format.read(list[0], version2);
                assertThat(read, equalTo(state2));
            } else {
                assertThat(list[0].getName(), anyOf(equalTo("foo-" + version + ".st"), equalTo("foo-" + version2 + ".st")));
                assertThat(list[1].getName(), anyOf(equalTo("foo-" + version + ".st"), equalTo("foo-" + version2 + ".st")));
                DummyState read = format.read(new File(stateDir, "foo-" + version2 + ".st"), version2);
                assertThat(read, equalTo(state2));
                read = format.read(new File(stateDir, "foo-" + version + ".st"), version);
                assertThat(read, equalTo(state));
            }

        }
    }

    @Test
    public void testVersionMismatch() throws IOException {
        File[] dirs = new File[randomIntBetween(1, 5)];
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = newTempDir(LifecycleScope.TEST);
        }
        final boolean deleteOldFiles = randomBoolean();
        Format format = new Format(randomFrom(XContentType.values()), deleteOldFiles);
        DummyState state = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 1000), randomInt(), randomLong(), randomDouble(), randomBoolean());
        int version = between(0, Integer.MAX_VALUE/2);
        format.write(state, "foo-", version, dirs);
        for (File file : dirs) {
            File[] list = file.listFiles();
            assertEquals(list.length, 1);
            assertThat(list[0].getName(), equalTo(MetaDataStateFormat.STATE_DIR_NAME));
            File stateDir = list[0];
            assertThat(stateDir.isDirectory(), is(true));
            list = stateDir.listFiles();
            assertEquals(list.length, 1);
            assertThat(list[0].getName(), equalTo("foo-" + version + ".st"));
            try {
                format.read(list[0], between(version+1, Integer.MAX_VALUE));
                fail("corruption expected");
            } catch (CorruptStateException ex) {
                // success
            }
            DummyState read = format.read(list[0], version);
            assertThat(read, equalTo(state));
        }
    }

    public void testCorruption() throws IOException {
        File[] dirs = new File[randomIntBetween(1, 5)];
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = newTempDir(LifecycleScope.TEST);
        }
        final boolean deleteOldFiles = randomBoolean();
        Format format = new Format(randomFrom(XContentType.values()), deleteOldFiles);
        DummyState state = new DummyState(randomRealisticUnicodeOfCodepointLengthBetween(1, 1000), randomInt(), randomLong(), randomDouble(), randomBoolean());
        int version = between(0, Integer.MAX_VALUE/2);
        format.write(state, "foo-", version, dirs);
        for (File file : dirs) {
            File[] list = file.listFiles();
            assertEquals(list.length, 1);
            assertThat(list[0].getName(), equalTo(MetaDataStateFormat.STATE_DIR_NAME));
            File stateDir = list[0];
            assertThat(stateDir.isDirectory(), is(true));
            list = stateDir.listFiles();
            assertEquals(list.length, 1);
            assertThat(list[0].getName(), equalTo("foo-" + version + ".st"));
            DummyState read = format.read(list[0], version);
            assertThat(read, equalTo(state));
            // now corrupt it
            corruptFile(list[0], logger);
            try {
                format.read(list[0], version);
                fail("corrupted file");
            } catch (CorruptStateException ex) {
                // expected
            }
        }
    }

    public static void corruptFile(File file, ESLogger logger) throws IOException {
        File fileToCorrupt = file;
        try (final SimpleFSDirectory dir = new SimpleFSDirectory(fileToCorrupt.getParentFile())) {
            long checksumBeforeCorruption;
            try (IndexInput input = dir.openInput(fileToCorrupt.getName(), IOContext.DEFAULT)) {
                checksumBeforeCorruption = CodecUtil.retrieveChecksum(input);
            }
            try (RandomAccessFile raf = new RandomAccessFile(fileToCorrupt, "rw")) {
                raf.seek(randomIntBetween(0, (int)Math.min(Integer.MAX_VALUE, raf.length()-1)));
                long filePointer = raf.getFilePointer();
                byte b = raf.readByte();
                raf.seek(filePointer);
                raf.writeByte(~b);
                raf.getFD().sync();
                logger.debug("Corrupting file {} --  flipping at position {} from {} to {} ", fileToCorrupt.getName(), filePointer, Integer.toHexString(b), Integer.toHexString(~b));
            }
        long checksumAfterCorruption;
        long actualChecksumAfterCorruption;
        try (ChecksumIndexInput input = dir.openChecksumInput(fileToCorrupt.getName(), IOContext.DEFAULT)) {
            assertThat(input.getFilePointer(), is(0l));
            input.seek(input.length() - 8); // one long is the checksum... 8 bytes
            checksumAfterCorruption = input.getChecksum();
            actualChecksumAfterCorruption = input.readLong();
        }
        StringBuilder msg = new StringBuilder();
        msg.append("Checksum before: [").append(checksumBeforeCorruption).append("]");
        msg.append(" after: [").append(checksumAfterCorruption).append("]");
        msg.append(" checksum value after corruption: ").append(actualChecksumAfterCorruption).append("]");
        msg.append(" file: ").append(fileToCorrupt.getName()).append(" length: ").append(dir.fileLength(fileToCorrupt.getName()));
        logger.debug(msg.toString());
        assumeTrue("Checksum collision - " + msg.toString(),
                checksumAfterCorruption != checksumBeforeCorruption // collision
                        || actualChecksumAfterCorruption != checksumBeforeCorruption); // checksum corrupted
        }
    }


    @Test
    public void testLoadState() throws IOException {
        final ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        final File[] dirs = new File[randomIntBetween(1, 5)];
        int numStates = randomIntBetween(1, 5);
        int numLegacy = randomIntBetween(0, numStates);
        List<MetaData> meta = new ArrayList<>();
        for (int i = 0; i < numStates; i++) {
            meta.add(randomMeta());
        }
        Set<File> corruptedFiles = new HashSet<>();
        MetaDataStateFormat<MetaData> format = LocalGatewayMetaState.globalStateFormat(randomFrom(XContentType.values()), params, randomBoolean());
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = newTempDir(LifecycleScope.TEST);
            Files.createDirectories(new File(dirs[i], MetaDataStateFormat.STATE_DIR_NAME).toPath());
            for (int j = 0; j < numLegacy; j++) {
                XContentType type = format.format();
                if (randomBoolean() && (j < numStates - 1 || dirs.length > 0 && i != 0)) {
                    File file = new File(new File(dirs[i], MetaDataStateFormat.STATE_DIR_NAME), "global-"+j);
                    Files.createFile(file.toPath()); // randomly create 0-byte files -- there is extra logic to skip them
                } else {
                    try (XContentBuilder xcontentBuilder = XContentFactory.contentBuilder(type, new FileOutputStream(new File(new File(dirs[i], MetaDataStateFormat.STATE_DIR_NAME), "global-" + j)))) {
                        xcontentBuilder.startObject();
                        MetaData.Builder.toXContent(meta.get(j), xcontentBuilder, params);
                        xcontentBuilder.endObject();
                    }
                }
            }
            for (int j = numLegacy; j < numStates; j++) {
                format.write(meta.get(j), LocalGatewayMetaState.GLOBAL_STATE_FILE_PREFIX, j, dirs[i]);
                if (randomBoolean() && (j < numStates - 1 || dirs.length > 0 && i != 0)) {  // corrupt a file that we do not necessarily need here....
                    File file = new File(new File(dirs[i], MetaDataStateFormat.STATE_DIR_NAME), "global-" + j + ".st");
                    corruptedFiles.add(file);
                    MetaDataStateFormatTest.corruptFile(file, logger);
                }
            }

        }
        List<File> dirList = Arrays.asList(dirs);
        Collections.shuffle(dirList, getRandom());
        MetaData loadedMetaData = MetaDataStateFormat.loadLatestState(logger, format, LocalGatewayMetaState.GLOBAL_STATE_FILE_PATTERN, "foobar", dirList.toArray(new File[0]));
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
                File file = new File(new File(dirs[i], MetaDataStateFormat.STATE_DIR_NAME), "global-" + (numStates-1) + ".st");
                if (corruptedFiles.contains(file)) {
                    continue;
                }
                MetaDataStateFormatTest.corruptFile(file, logger);
            }
            try {
                MetaDataStateFormat.loadLatestState(logger, format, LocalGatewayMetaState.GLOBAL_STATE_FILE_PATTERN, "foobar", dirList.toArray(new File[0]));
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
                .settings(ImmutableSettings.settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10)).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 5)));
    }


    private class Format extends MetaDataStateFormat<DummyState> {

        Format(XContentType format, boolean deleteOldFiles) {
            super(format, deleteOldFiles);
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
        protected Directory newDirectory(File dir) throws IOException {
            MockDirectoryWrapper  mock = new MockDirectoryWrapper(getRandom(), super.newDirectory(dir));
            closeAfterSuite(new CloseableDirectory(mock, suiteFailureMarker));
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
}
