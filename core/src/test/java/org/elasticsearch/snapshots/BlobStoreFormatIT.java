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

package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchCorruptionException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.repositories.blobstore.LegacyBlobStoreFormat;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.*;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

public class BlobStoreFormatIT extends AbstractSnapshotIntegTestCase {

    private static final ParseFieldMatcher parseFieldMatcher = new ParseFieldMatcher(Settings.EMPTY);

    public static final String BLOB_CODEC = "blob";

    private static class BlobObj implements ToXContent, FromXContentBuilder<BlobObj> {
        public static final BlobObj PROTO = new BlobObj("");

        private final String text;

        public BlobObj(String text) {
            this.text = text;
        }

        public String getText() {
            return text;
        }

        @Override
        public BlobObj fromXContent(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
            String text = null;
            XContentParser.Token token = parser.currentToken();
            if (token == null) {
                token = parser.nextToken();
            }
            if (token == XContentParser.Token.START_OBJECT) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token != XContentParser.Token.FIELD_NAME) {
                        throw new ElasticsearchParseException("unexpected token [{}]", token);
                    }
                    String currentFieldName = parser.currentName();
                    token = parser.nextToken();
                    if (token.isValue()) {
                        if ("text" .equals(currentFieldName)) {
                            text = parser.text();
                        } else {
                            throw new ElasticsearchParseException("unexpected field [{}]", currentFieldName);
                        }
                    } else {
                        throw new ElasticsearchParseException("unexpected token [{}]", token);
                    }
                }
            }
            if (text == null) {
                throw new ElasticsearchParseException("missing mandatory parameter text");
            }
            return new BlobObj(text);
        }

        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.field("text", getText());
            return builder;
        }
    }

    /**
     * Extends legacy format with writing functionality. It's used to simulate legacy file formats in tests.
     */
    private static final class LegacyEmulationBlobStoreFormat<T extends ToXContent> extends LegacyBlobStoreFormat<T> {

        protected final XContentType xContentType;

        protected final boolean compress;

        public LegacyEmulationBlobStoreFormat(String blobNameFormat, FromXContentBuilder<T> reader, ParseFieldMatcher parseFieldMatcher, boolean compress, XContentType xContentType) {
            super(blobNameFormat, reader, parseFieldMatcher);
            this.xContentType = xContentType;
            this.compress = compress;
        }

        public void write(T obj, BlobContainer blobContainer, String blobName) throws IOException {
            BytesReference bytes = write(obj);
            blobContainer.writeBlob(blobName, bytes);
        }

        private BytesReference write(T obj) throws IOException {
            try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
                if (compress) {
                    try (StreamOutput compressedStreamOutput = CompressorFactory.defaultCompressor().streamOutput(bytesStreamOutput)) {
                        write(obj, compressedStreamOutput);
                    }
                } else {
                    write(obj, bytesStreamOutput);
                }
                return bytesStreamOutput.bytes();
            }
        }

        private void write(T obj, StreamOutput streamOutput) throws IOException {
            XContentBuilder builder = XContentFactory.contentBuilder(xContentType, streamOutput);
            builder.startObject();
            obj.toXContent(builder, SNAPSHOT_ONLY_FORMAT_PARAMS);
            builder.endObject();
            builder.close();
        }
    }

    @Test
    public void testBlobStoreOperations() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        ChecksumBlobStoreFormat<BlobObj> checksumJSON = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj.PROTO, parseFieldMatcher, false, XContentType.JSON);
        ChecksumBlobStoreFormat<BlobObj> checksumSMILE = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj.PROTO, parseFieldMatcher, false, XContentType.SMILE);
        ChecksumBlobStoreFormat<BlobObj> checksumSMILECompressed = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj.PROTO, parseFieldMatcher, true, XContentType.SMILE);
        LegacyEmulationBlobStoreFormat<BlobObj> legacyJSON = new LegacyEmulationBlobStoreFormat<>("%s", BlobObj.PROTO, parseFieldMatcher, false, XContentType.JSON);
        LegacyEmulationBlobStoreFormat<BlobObj> legacySMILE = new LegacyEmulationBlobStoreFormat<>("%s", BlobObj.PROTO, parseFieldMatcher, false, XContentType.SMILE);
        LegacyEmulationBlobStoreFormat<BlobObj> legacySMILECompressed = new LegacyEmulationBlobStoreFormat<>("%s", BlobObj.PROTO, parseFieldMatcher, true, XContentType.SMILE);

        // Write blobs in different formats
        checksumJSON.write(new BlobObj("checksum json"), blobContainer, "check-json");
        checksumSMILE.write(new BlobObj("checksum smile"), blobContainer, "check-smile");
        checksumSMILECompressed.write(new BlobObj("checksum smile compressed"), blobContainer, "check-smile-comp");
        legacyJSON.write(new BlobObj("legacy json"), blobContainer, "legacy-json");
        legacySMILE.write(new BlobObj("legacy smile"), blobContainer, "legacy-smile");
        legacySMILECompressed.write(new BlobObj("legacy smile compressed"), blobContainer, "legacy-smile-comp");

        // Assert that all checksum blobs can be read by all formats
        assertEquals(checksumJSON.read(blobContainer, "check-json").getText(), "checksum json");
        assertEquals(checksumSMILE.read(blobContainer, "check-json").getText(), "checksum json");
        assertEquals(checksumJSON.read(blobContainer, "check-smile").getText(), "checksum smile");
        assertEquals(checksumSMILE.read(blobContainer, "check-smile").getText(), "checksum smile");
        assertEquals(checksumJSON.read(blobContainer, "check-smile-comp").getText(), "checksum smile compressed");
        assertEquals(checksumSMILE.read(blobContainer, "check-smile-comp").getText(), "checksum smile compressed");

        // Assert that all legacy blobs can be read be all formats
        assertEquals(legacyJSON.read(blobContainer, "legacy-json").getText(), "legacy json");
        assertEquals(legacySMILE.read(blobContainer, "legacy-json").getText(), "legacy json");
        assertEquals(legacyJSON.read(blobContainer, "legacy-smile").getText(), "legacy smile");
        assertEquals(legacySMILE.read(blobContainer, "legacy-smile").getText(), "legacy smile");
        assertEquals(legacyJSON.read(blobContainer, "legacy-smile-comp").getText(), "legacy smile compressed");
        assertEquals(legacySMILE.read(blobContainer, "legacy-smile-comp").getText(), "legacy smile compressed");
    }


    @Test
    public void testCompressionIsApplied() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        StringBuilder veryRedundantText = new StringBuilder();
        for (int i = 0; i < randomIntBetween(100, 300); i++) {
            veryRedundantText.append("Blah ");
        }
        ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj.PROTO, parseFieldMatcher, false, randomBoolean() ? XContentType.SMILE : XContentType.JSON);
        ChecksumBlobStoreFormat<BlobObj> checksumFormatComp = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj.PROTO, parseFieldMatcher, true, randomBoolean() ? XContentType.SMILE : XContentType.JSON);
        BlobObj blobObj = new BlobObj(veryRedundantText.toString());
        checksumFormatComp.write(blobObj, blobContainer, "blob-comp");
        checksumFormat.write(blobObj, blobContainer, "blob-not-comp");
        Map<String, BlobMetaData> blobs = blobContainer.listBlobsByPrefix("blob-");
        assertEquals(blobs.size(), 2);
        assertThat(blobs.get("blob-not-comp").length(), greaterThan(blobs.get("blob-comp").length()));
    }

    @Test
    public void testBlobCorruption() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        String testString = randomAsciiOfLength(randomInt(10000));
        BlobObj blobObj = new BlobObj(testString);
        ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj.PROTO, parseFieldMatcher, randomBoolean(), randomBoolean() ? XContentType.SMILE : XContentType.JSON);
        checksumFormat.write(blobObj, blobContainer, "test-path");
        assertEquals(checksumFormat.read(blobContainer, "test-path").getText(), testString);
        randomCorruption(blobContainer, "test-path");
        try {
            checksumFormat.read(blobContainer, "test-path");
            fail("Should have failed due to corruption");
        } catch (ElasticsearchCorruptionException ex) {
            assertThat(ex.getMessage(), containsString("test-path"));
        } catch (EOFException ex) {
            // This can happen if corrupt the byte length
        }
    }

    public void testAtomicWrite() throws Exception {
        final BlobStore blobStore = createTestBlobStore();
        final BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        String testString = randomAsciiOfLength(randomInt(10000));
        final CountDownLatch block = new CountDownLatch(1);
        final CountDownLatch unblock = new CountDownLatch(1);
        final BlobObj blobObj = new BlobObj(testString) {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
                super.toXContent(builder, params);
                // Block before finishing writing
                try {
                    block.countDown();
                    unblock.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                return builder;
            }
        };
        final ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj.PROTO, parseFieldMatcher, randomBoolean(), randomBoolean() ? XContentType.SMILE : XContentType.JSON);
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        try {
            Future<Void> future = threadPool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    checksumFormat.writeAtomic(blobObj, blobContainer, "test-blob");
                    return null;
                }
            });
            block.await(5, TimeUnit.SECONDS);
            assertFalse(blobContainer.blobExists("test-blob"));
            unblock.countDown();
            future.get();
            assertTrue(blobContainer.blobExists("test-blob"));
        } finally {
            threadPool.shutdown();
        }
    }

    protected BlobStore createTestBlobStore() throws IOException {
        Settings settings = Settings.builder().build();
        return new FsBlobStore(settings, randomRepoPath());
    }

    protected void randomCorruption(BlobContainer blobContainer, String blobName) throws IOException {
        byte[] buffer = new byte[(int) blobContainer.listBlobsByPrefix(blobName).get(blobName).length()];
        long originalChecksum = checksum(buffer);
        try (InputStream inputStream = blobContainer.readBlob(blobName)) {
            Streams.readFully(inputStream, buffer);
        }
        do {
            int location = randomIntBetween(0, buffer.length - 1);
            buffer[location] = (byte) (buffer[location] ^ 42);
        } while (originalChecksum == checksum(buffer));
        blobContainer.writeBlob(blobName, new BytesArray(buffer));
    }

    private long checksum(byte[] buffer) throws IOException {
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            try (BufferedChecksumStreamOutput checksumOutput = new BufferedChecksumStreamOutput(streamOutput)) {
                checksumOutput.write(buffer);
                return checksumOutput.getChecksum();
            }
        }
    }

}
