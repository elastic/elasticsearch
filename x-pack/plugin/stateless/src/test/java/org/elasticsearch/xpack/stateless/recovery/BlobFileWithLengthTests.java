/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit.blobNameFromGeneration;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomNonZeroPositiveLong;

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
            return new BlobFileWithLength(
                randomValueOtherThan(instance.blobFile(), BlobFileWithLengthTests::randomBlobFile),
                instance.length()
            );
        }
        return new BlobFileWithLength(instance.blobFile(), randomValueOtherThan(instance.length(), () -> randomNonNegativeLong()));
    }

    private static BlobFile randomBlobFile() {
        var termAndGeneration = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomNonZeroPositiveLong());
        return new BlobFile(blobNameFromGeneration(termAndGeneration.generation()), termAndGeneration);
    }
}
