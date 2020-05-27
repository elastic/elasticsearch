/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.store.checksum;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;

import static org.hamcrest.Matchers.equalTo;

public class ChecksumBlobContainerIndexInputTests extends ESTestCase {

    public void testChecksumBlobContainerIndexInput() throws Exception {
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try (IndexOutput output = new ByteBuffersIndexOutput(out, "tmp", getTestName())) {
            CodecUtil.writeHeader(output, getTestName(), 5);
            output.writeString(randomRealisticUnicodeOfLengthBetween(1, 50));
            CodecUtil.writeFooter(output);
        }

        final byte[] bytes = out.toArrayCopy();
        final long checksum = CodecUtil.checksumEntireFile(new ByteArrayIndexInput(getTestName(), bytes));

        final ChecksumBlobContainerIndexInput indexInput = ChecksumBlobContainerIndexInput.create(
            getTestName(),
            bytes.length,
            Store.digestToString(checksum),
            Store.READONCE_CHECKSUM
        );
        assertThat(indexInput.length(), equalTo((long) bytes.length));
        assertThat(indexInput.getFilePointer(), equalTo(0L));
        assertThat(CodecUtil.retrieveChecksum(indexInput), equalTo(checksum));
        assertThat(indexInput.getFilePointer(), equalTo((long) bytes.length));

        expectThrows(EOFException.class, () -> indexInput.readByte());
        expectThrows(EOFException.class, () -> indexInput.readBytes(new byte[0], 0, 1));
        expectThrows(EOFException.class, () -> indexInput.seek(bytes.length + 1));
    }
}
