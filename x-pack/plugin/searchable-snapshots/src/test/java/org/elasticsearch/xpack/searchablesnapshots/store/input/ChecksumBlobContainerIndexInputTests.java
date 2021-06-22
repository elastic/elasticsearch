/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.lucene.codecs.CodecUtil;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;

import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomChecksumBytes;
import static org.hamcrest.Matchers.equalTo;

public class ChecksumBlobContainerIndexInputTests extends ESTestCase {

    public void testChecksumBlobContainerIndexInput() throws Exception {
        final Tuple<String, byte[]> bytes = randomChecksumBytes(randomIntBetween(1, 100));
        final long length = bytes.v2().length;
        final String checksum = bytes.v1();

        final ChecksumBlobContainerIndexInput indexInput = ChecksumBlobContainerIndexInput.create(
            getTestName(),
            length,
            checksum,
            Store.READONCE_CHECKSUM
        );
        assertThat(indexInput.length(), equalTo(length));
        assertThat(indexInput.getFilePointer(), equalTo(0L));
        assertThat(CodecUtil.retrieveChecksum(indexInput), equalTo(Long.parseLong(checksum, Character.MAX_RADIX)));
        assertThat(indexInput.getFilePointer(), equalTo(length));

        expectThrows(EOFException.class, indexInput::readByte);
        expectThrows(EOFException.class, () -> indexInput.readBytes(new byte[0], 0, 1));
        expectThrows(EOFException.class, () -> indexInput.seek(length + 1L));
    }
}
