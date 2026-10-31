/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.commits.BccBlobFile;
import org.elasticsearch.xpack.stateless.commits.MergeBlobFile;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.commits.BlobFileTestUtils.randomBlobFile;

public class BlobFileWithLengthTests extends AbstractWireSerializingTestCase<BlobFileWithLength> {

    @Override
    protected Writeable.Reader<BlobFileWithLength> instanceReader() {
        return BlobFileWithLength::new;
    }

    @Override
    protected BlobFileWithLength createTestInstance() {
        return new BlobFileWithLength(randomBlobFile(), randomNonNegativeLong());
    }

    @Override
    protected BlobFileWithLength mutateInstance(BlobFileWithLength instance) throws IOException {
        if (randomBoolean()) {
            return new BlobFileWithLength(randomValueOtherThan(instance.blobFile(), () -> randomBlobFile()), instance.length());
        }
        return new BlobFileWithLength(instance.blobFile(), randomValueOtherThan(instance.length(), () -> randomNonNegativeLong()));
    }

    public void testBothKindsOfBlobFileAreCovered() {
        boolean bcc = false;
        boolean merge = false;
        for (int i = 0; i < 100 && (bcc == false || merge == false); i++) {
            switch (randomBlobFile()) {
                case BccBlobFile ignored -> bcc = true;
                case MergeBlobFile ignored -> merge = true;
            }
        }
        assertTrue("random blob files should include BCC blobs", bcc);
        assertTrue("random blob files should include merge blobs", merge);
    }
}
