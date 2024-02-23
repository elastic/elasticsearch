/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

final class VerifyingIndexOutput extends FilterIndexOutput {

    private static final int CHECKSUM_LENGTH = 8;

    private final StoreFileMetadata metadata;
    private long writtenBytes;
    private final long checksumPosition;
    private final ByteBuffer computedChecksum = ByteBuffer.allocate(CHECKSUM_LENGTH); // computed from the bytes written before checksum
    private final ByteBuffer footerChecksum = ByteBuffer.allocate(CHECKSUM_LENGTH); // accumulates the bytes written to the checksum

    VerifyingIndexOutput(StoreFileMetadata metadata, IndexOutput out) throws IOException {
        super("VerifyingIndexOutput(out=" + out.toString() + ")", out);
        this.metadata = metadata;
        this.checksumPosition = metadata.length() - CHECKSUM_LENGTH; // the checksum is at the very end
        this.computedChecksum.order(ByteOrder.BIG_ENDIAN);
        this.footerChecksum.order(ByteOrder.BIG_ENDIAN);

        if (writtenBytes == checksumPosition) {
            // 8-byte file is invalid for other reasons, but maintain invariants here anyway
            computeChecksum();
        }
    }

    @Override
    public void writeByte(byte b) throws IOException {
        if (checksumPosition <= writtenBytes && writtenBytes < checksumPosition + CHECKSUM_LENGTH) {
            // we are writing the checksum
            footerChecksum.put(b);
        }

        out.writeByte(b);
        writtenBytes += 1;

        if (writtenBytes == checksumPosition) {
            computeChecksum();
        }
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        assert 0 < length;

        if (writtenBytes < checksumPosition) {
            // we are writing the body (before the checksum)
            final var lengthToCopy = Math.toIntExact(Math.min(length, checksumPosition - writtenBytes)); // no overflow
            out.writeBytes(b, offset, lengthToCopy);
            writtenBytes += lengthToCopy;
            offset += lengthToCopy;
            length -= lengthToCopy;

            if (writtenBytes == checksumPosition) {
                computeChecksum();
            } else {
                assert length == 0;
                assert writtenBytes < checksumPosition;
            }
        }

        if (0 < length) {
            assert writtenBytes >= checksumPosition; // past start of checksum

            final var checksumIndex = Math.toIntExact(Math.min(CHECKSUM_LENGTH, writtenBytes - checksumPosition)); // no overflow
            if (checksumIndex < CHECKSUM_LENGTH) {
                // we are writing the checksum
                final var lengthToCopy = Math.toIntExact(Math.min(length, CHECKSUM_LENGTH - checksumIndex)); // no overflow
                footerChecksum.put(b, offset, lengthToCopy);
                out.writeBytes(b, offset, lengthToCopy);
                writtenBytes += lengthToCopy;
                offset += lengthToCopy;
                length -= lengthToCopy;
            }
        }

        if (0 < length) {
            assert writtenBytes >= checksumPosition + CHECKSUM_LENGTH; // past end of checksum
            out.writeBytes(b, offset, length);
            writtenBytes += length;
        }
    }

    private void computeChecksum() throws IOException {
        assert writtenBytes == checksumPosition : writtenBytes + " vs " + checksumPosition;
        assert computedChecksum.position() == 0;
        computedChecksum.putLong(getChecksum());
    }

    private static String checksumString(ByteBuffer byteBuffer) {
        assert byteBuffer.remaining() == 0;
        byteBuffer.flip();
        assert byteBuffer.remaining() == CHECKSUM_LENGTH;
        return Store.digestToString(byteBuffer.getLong());
    }

    @Nullable // if valid
    private String getChecksumsIfInvalid() {
        if (writtenBytes != metadata.length()) {
            return "actual=<invalid length> footer=<invalid length>";
        }

        if (metadata.length() < CHECKSUM_LENGTH) {
            return "actual=<too short> footer=<too short>";
        }

        assert computedChecksum.remaining() == 0;

        final var computedChecksumString = checksumString(computedChecksum);
        if (metadata.checksum().equals(computedChecksumString) == false) {
            return Strings.format("actual=%s footer=<not checked>", computedChecksumString);
        }

        final var footerChecksumString = checksumString(footerChecksum);
        if (metadata.checksum().equals(footerChecksumString)) {
            return null;
        } else {
            return Strings.format("actual=%s footer=%s", computedChecksumString, footerChecksumString);
        }
    }

    void verify() throws IOException {
        final var checksumsIfInvalid = getChecksumsIfInvalid();
        if (checksumsIfInvalid != null) {
            throw new CorruptIndexException(
                Strings.format(
                    "verification failed (hardware problem?) : expected=%s %s writtenLength=%d expectedLength=%d (resource=%s)",
                    metadata.checksum(),
                    checksumsIfInvalid,
                    writtenBytes,
                    metadata.length(),
                    metadata
                ),
                "VerifyingIndexOutput(" + metadata.name() + ")"
            );
        }
    }
}
