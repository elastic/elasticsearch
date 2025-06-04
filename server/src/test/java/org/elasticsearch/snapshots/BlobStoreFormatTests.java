/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Streams;
import org.elasticsearch.exception.ElasticsearchCorruptionException;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.greaterThan;

public class BlobStoreFormatTests extends ESTestCase {

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
                    XContentParserUtils.ensureFieldName(parser, token, "text");
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(), parser);
                    text = parser.text();
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
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.EMPTY);
        ChecksumBlobStoreFormat<BlobObj> checksumSMILE = new ChecksumBlobStoreFormat<>(
            BLOB_CODEC,
            "%s",
            (repoName, parser) -> BlobObj.fromXContent(parser),
            Function.identity()
        );

        // Write blobs in different formats
        final String randomText = randomAlphaOfLengthBetween(0, 1024 * 8 * 3);
        final String normalText = "checksum smile: " + randomText;
        checksumSMILE.write(new BlobObj(normalText), blobContainer, "check-smile", false);
        final String compressedText = "checksum smile compressed: " + randomText;
        checksumSMILE.write(new BlobObj(compressedText), blobContainer, "check-smile-comp", true);

        // Assert that all checksum blobs can be read
        assertEquals(normalText, checksumSMILE.read("repo", blobContainer, "check-smile", xContentRegistry()).getText());
        assertEquals(compressedText, checksumSMILE.read("repo", blobContainer, "check-smile-comp", xContentRegistry()).getText());
    }

    public void testCompressionIsApplied() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.EMPTY);
        StringBuilder veryRedundantText = new StringBuilder();
        for (int i = 0; i < randomIntBetween(100, 300); i++) {
            veryRedundantText.append("Blah ");
        }
        ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(
            BLOB_CODEC,
            "%s",
            (repo, parser) -> BlobObj.fromXContent(parser),
            Function.identity()
        );
        BlobObj blobObj = new BlobObj(veryRedundantText.toString());
        checksumFormat.write(blobObj, blobContainer, "blob-comp", true);
        checksumFormat.write(blobObj, blobContainer, "blob-not-comp", false);
        Map<String, BlobMetadata> blobs = blobContainer.listBlobsByPrefix(randomPurpose(), "blob-");
        assertEquals(blobs.size(), 2);
        assertThat(blobs.get("blob-not-comp").length(), greaterThan(blobs.get("blob-comp").length()));
    }

    public void testBlobCorruption() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.EMPTY);
        String testString = randomAlphaOfLength(randomInt(10000));
        BlobObj blobObj = new BlobObj(testString);

        ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(
            BLOB_CODEC,
            "%s",
            (repo, parser) -> BlobObj.fromXContent(parser),
            Function.identity()
        );
        checksumFormat.write(blobObj, blobContainer, "test-path", randomBoolean());
        assertEquals(checksumFormat.read("repo", blobContainer, "test-path", xContentRegistry()).getText(), testString);
        randomCorruption(blobContainer, "test-path");
        try {
            checksumFormat.read("repo", blobContainer, "test-path", xContentRegistry());
            fail("Should have failed due to corruption");
        } catch (ElasticsearchCorruptionException | EOFException ex) {
            // expected exceptions from random byte corruption
        }
    }

    protected BlobStore createTestBlobStore() throws IOException {
        return new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
    }

    protected void randomCorruption(BlobContainer blobContainer, String blobName) throws IOException {
        final byte[] buffer = new byte[(int) blobContainer.listBlobsByPrefix(randomPurpose(), blobName).get(blobName).length()];
        try (InputStream inputStream = blobContainer.readBlob(randomPurpose(), blobName)) {
            Streams.readFully(inputStream, buffer);
        }
        final BytesArray corruptedBytes;
        final int location = randomIntBetween(0, buffer.length - 1);
        if (randomBoolean()) {
            // flipping bits in a single byte will always invalidate the file: CRC-32 certainly detects all eight-bit-burst errors; we don't
            // checksum the last 8 bytes but we do verify that they contain the checksum preceded by 4 zero bytes so in any case this will
            // be a detectable error:
            buffer[location] = (byte) (buffer[location] ^ between(1, 255));
            corruptedBytes = new BytesArray(buffer);
        } else {
            // truncation will invalidate the file: the last 12 bytes should start with 8 zero bytes but by construction we won't have
            // another sequence of 8 zero bytes anywhere in the file, let alone such a sequence followed by a correct checksum.
            corruptedBytes = new BytesArray(buffer, 0, location);
        }
        blobContainer.writeBlob(randomPurpose(), blobName, corruptedBytes, false);
    }

}
