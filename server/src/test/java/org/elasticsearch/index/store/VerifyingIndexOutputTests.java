/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class VerifyingIndexOutputTests extends ESTestCase {

    private static final int CHECKSUM_LENGTH = 8;

    private static final Version MIN_SUPPORTED_LUCENE_VERSION = IndexVersion.MINIMUM_COMPATIBLE.luceneVersion();
    private static final Matcher<String> VERIFICATION_FAILURE = containsString("verification failed (hardware problem?)");
    private static final Matcher<String> FOOTER_NOT_CHECKED = allOf(VERIFICATION_FAILURE, containsString("footer=<not checked>"));
    private static final Matcher<String> INVALID_LENGTH = allOf(VERIFICATION_FAILURE, containsString("footer=<invalid length>"));

    public void testVerifyingIndexOutput() throws IOException {
        try (var directory = newDirectory()) {
            final StoreFileMetadata metadata = createFileWithChecksum(directory);

            try (var verifyingOutput = new VerifyingIndexOutput(metadata, directory.createOutput("rewritten.dat", IOContext.DEFAULT))) {
                try (var indexInput = directory.openInput(metadata.name(), IOContext.DEFAULT)) {
                    final byte[] buffer = new byte[1024];
                    int length = Math.toIntExact(metadata.length());
                    while (length > 0) {
                        if (random().nextInt(10) == 0) {
                            verifyingOutput.writeByte(indexInput.readByte());
                            length--;
                        } else {
                            int offset = between(0, buffer.length - 1);
                            int len = between(1, Math.min(length, buffer.length - offset));
                            indexInput.readBytes(buffer, offset, len);
                            verifyingOutput.writeBytes(buffer, offset, len);
                            length -= len;
                        }
                    }
                }

                for (int i = 0; i < 2; i++) {
                    // check twice to make sure the first call doesn't leave things in a broken state
                    verifyingOutput.verify(); // should not throw
                }

                ensureCorruptAfterRandomWrite(verifyingOutput);
            }
        }
    }

    public void testVerifyingIndexOutputOnTooShortFile() throws IOException {
        final var metadata = new StoreFileMetadata(
            "foo.bar",
            between(0, CHECKSUM_LENGTH - 1),
            randomAlphaOfLength(5),
            MIN_SUPPORTED_LUCENE_VERSION.toString()
        );
        try (
            var dir = newDirectory();
            var verifyingOutput = new VerifyingIndexOutput(metadata, dir.createOutput("rewritten.dat", IOContext.DEFAULT))
        ) {

            int length = Math.toIntExact(metadata.length());
            final ByteBuffer buffer = ByteBuffer.wrap(randomByteArrayOfLength(length));
            while (buffer.remaining() > 0) {
                // verify should fail on truncated file
                assertThat(expectThrows(CorruptIndexException.class, verifyingOutput::verify).getMessage(), INVALID_LENGTH);

                if (randomBoolean()) {
                    verifyingOutput.writeByte(buffer.get());
                } else {
                    var lenToWrite = between(1, buffer.remaining());
                    verifyingOutput.writeBytes(buffer.array(), buffer.arrayOffset() + buffer.position(), lenToWrite);
                    buffer.position(buffer.position() + lenToWrite);
                }
            }

            for (int i = 0; i < 2; i++) {
                // check twice to make sure the first call doesn't leave things in a broken state
                assertThat(
                    expectThrows(CorruptIndexException.class, verifyingOutput::verify).getMessage(),
                    allOf(VERIFICATION_FAILURE, containsString("actual=<too short>"))
                );
            }

            ensureCorruptAfterRandomWrite(verifyingOutput);
        }
    }

    public void testVerifyingIndexOutputOnCorruptFile() throws IOException {
        try (var dir = newDirectory()) {
            final StoreFileMetadata metadata = createFileWithChecksum(dir);

            int length = Math.toIntExact(metadata.length());
            final var byteToCorrupt = between(0, length - 1);
            final var isExpectedMessage = byteToCorrupt < length - CHECKSUM_LENGTH ? FOOTER_NOT_CHECKED : VERIFICATION_FAILURE;

            try (var verifyingOutput = new VerifyingIndexOutput(metadata, dir.createOutput("rewritten.dat", IOContext.DEFAULT))) {
                try (var indexInput = dir.openInput(metadata.name(), IOContext.DEFAULT)) {
                    final byte[] buffer = new byte[1024];
                    while (length > 0) {

                        // verify should fail on truncated file
                        assertThat(expectThrows(CorruptIndexException.class, verifyingOutput::verify).getMessage(), INVALID_LENGTH);

                        int offset = between(0, buffer.length - 1);
                        var singleByte = randomInt(10) == 0;
                        var len = singleByte ? 1 : between(1, Math.min(length, buffer.length - offset));
                        indexInput.readBytes(buffer, offset, len);

                        if (verifyingOutput.getFilePointer() <= byteToCorrupt && byteToCorrupt < indexInput.getFilePointer()) {
                            // CRC32 will always detect single-bit errors
                            final byte oneBit = (byte) (1 << between(0, Byte.SIZE - 1));
                            buffer[offset + byteToCorrupt - Math.toIntExact(verifyingOutput.getFilePointer())] ^= oneBit;
                        }

                        if (singleByte) {
                            verifyingOutput.writeByte(buffer[offset]);
                        } else {
                            verifyingOutput.writeBytes(buffer, offset, len);
                        }

                        length -= len;
                    }
                }

                assertEquals(metadata.length(), verifyingOutput.getFilePointer());

                for (int i = 0; i < 2; i++) {
                    // check twice to make sure the first call doesn't leave things in a broken state
                    assertThat(expectThrows(CorruptIndexException.class, verifyingOutput::verify).getMessage(), isExpectedMessage);
                }

                ensureCorruptAfterRandomWrite(verifyingOutput);
            }
        }
    }

    private static StoreFileMetadata createFileWithChecksum(Directory directory) throws IOException {
        final var filename = randomAlphaOfLength(10);
        final var bytes = randomByteArrayOfLength(scaledRandomIntBetween(0, 2048));
        try (var output = directory.createOutput(filename, IOContext.DEFAULT)) {
            output.writeBytes(bytes, bytes.length);
            if (rarely()) {
                // in practice there's always a complete 16-byte footer, but this ensures that we only care about the checksum
                final var checksum = output.getChecksum();
                final var metadata = new StoreFileMetadata(
                    filename,
                    bytes.length + Long.BYTES,
                    Store.digestToString(checksum),
                    MIN_SUPPORTED_LUCENE_VERSION.toString()
                );
                output.writeBytes(Numbers.longToBytes(checksum), Long.BYTES);
                return metadata;
            } else {
                CodecUtil.writeFooter(output);
                // fall through and obtain the checksum using an IndexInput
            }
        }

        try (var indexInput = directory.openInput(filename, IOContext.DEFAULT)) {
            assertEquals(bytes.length + CodecUtil.footerLength(), indexInput.length());
            return new StoreFileMetadata(
                filename,
                indexInput.length(),
                Store.digestToString(CodecUtil.retrieveChecksum(indexInput)),
                MIN_SUPPORTED_LUCENE_VERSION.toString()
            );
        }
    }

    private static void ensureCorruptAfterRandomWrite(VerifyingIndexOutput verifyingOutput) throws IOException {
        if (randomBoolean()) {
            verifyingOutput.writeByte(randomByte());
        } else {
            final var len = between(1, 10);
            verifyingOutput.writeBytes(randomByteArrayOfLength(len), 0, len);
        }
        assertThat(expectThrows(CorruptIndexException.class, verifyingOutput::verify).getMessage(), INVALID_LENGTH);
    }
}
