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
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.snapshots.mockstore.BlobContainerWrapper;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

public class BlobStoreFormatIT extends AbstractSnapshotIntegTestCase {

    public static final String BLOB_CODEC = "blob";

    private static class BlobObj implements ToXContentFragment {

        private final String text;

        BlobObj(String text) {
            this.text = text;
        }

        public String getText() {
            return text;
        }

        public static BlobObj fromXContent(XContentParser parser) throws IOException {
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.field("text", getText());
            return builder;
        }
    }

    public void testBlobStoreOperations() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        ChecksumBlobStoreFormat<BlobObj> checksumSMILE = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent,
            xContentRegistry(), false);
        ChecksumBlobStoreFormat<BlobObj> checksumSMILECompressed = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent,
            xContentRegistry(), true);

        // Write blobs in different formats
        checksumSMILE.write(new BlobObj("checksum smile"), blobContainer, "check-smile", true);
        checksumSMILECompressed.write(new BlobObj("checksum smile compressed"), blobContainer, "check-smile-comp", true);

        // Assert that all checksum blobs can be read by all formats
        assertEquals(checksumSMILE.read(blobContainer, "check-smile").getText(), "checksum smile");
        assertEquals(checksumSMILE.read(blobContainer, "check-smile-comp").getText(), "checksum smile compressed");
    }

    public void testCompressionIsApplied() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        StringBuilder veryRedundantText = new StringBuilder();
        for (int i = 0; i < randomIntBetween(100, 300); i++) {
            veryRedundantText.append("Blah ");
        }
        ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent,
            xContentRegistry(), false);
        ChecksumBlobStoreFormat<BlobObj> checksumFormatComp = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent,
            xContentRegistry(), true);
        BlobObj blobObj = new BlobObj(veryRedundantText.toString());
        checksumFormatComp.write(blobObj, blobContainer, "blob-comp", true);
        checksumFormat.write(blobObj, blobContainer, "blob-not-comp", true);
        Map<String, BlobMetaData> blobs = blobContainer.listBlobsByPrefix("blob-");
        assertEquals(blobs.size(), 2);
        assertThat(blobs.get("blob-not-comp").length(), greaterThan(blobs.get("blob-comp").length()));
    }

    public void testBlobCorruption() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        String testString = randomAlphaOfLength(randomInt(10000));
        BlobObj blobObj = new BlobObj(testString);
        ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent,
            xContentRegistry(), randomBoolean());
        checksumFormat.write(blobObj, blobContainer, "test-path", true);
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
        String testString = randomAlphaOfLength(randomInt(10000));
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
        final ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent,
            xContentRegistry(), randomBoolean());
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        try {
            Future<Void> future = threadPool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    checksumFormat.writeAtomic(blobObj, blobContainer, "test-blob");
                    return null;
                }
            });
            // signalling
            block.await(5, TimeUnit.SECONDS);
            assertFalse(BlobStoreTestUtil.blobExists(blobContainer, "test-blob"));
            unblock.countDown();
            future.get();
            assertTrue(BlobStoreTestUtil.blobExists(blobContainer, "test-blob"));
        } finally {
            threadPool.shutdown();
        }
    }

    public void testAtomicWriteFailures() throws Exception {
        final String name = randomAlphaOfLength(10);
        final BlobObj blobObj = new BlobObj("test");
        final ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent,
            xContentRegistry(), randomBoolean());

        final BlobStore blobStore = createTestBlobStore();
        final BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());

        {
            IOException writeBlobException = expectThrows(IOException.class, () -> {
                BlobContainer wrapper = new BlobContainerWrapper(blobContainer) {
                    @Override
                    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                        throws IOException {
                        throw new IOException("Exception thrown in writeBlobAtomic() for " + blobName);
                    }
                };
                checksumFormat.writeAtomic(blobObj, wrapper, name);
            });

            assertEquals("Exception thrown in writeBlobAtomic() for " + name, writeBlobException.getMessage());
            assertEquals(0, writeBlobException.getSuppressed().length);
        }
    }

    protected BlobStore createTestBlobStore() throws IOException {
        return new FsBlobStore(Settings.EMPTY, randomRepoPath(), false);
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
        BytesArray bytesArray = new BytesArray(buffer);
        try (StreamInput stream = bytesArray.streamInput()) {
            blobContainer.writeBlob(blobName, stream, bytesArray.length(), false);
        }
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
