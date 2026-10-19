/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.commits.BccBlobFile.blobNameFromGeneration;
import static org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests.mutatePrimaryTermAndGeneration;
import static org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests.randomPrimaryTermAndGeneration;

public class BlobFileTests extends AbstractWireSerializingTestCase<BlobFile> {
    @Override
    protected Writeable.Reader<BlobFile> instanceReader() {
        return BlobFile::readFrom;
    }

    @Override
    protected BlobFile createTestInstance() {
        return randomBlobFile();
    }

    private BlobFile randomBlobFile() {
        var termAndGen = randomPrimaryTermAndGeneration();
        return new BccBlobFile(termAndGen);
    }

    @Override
    protected BlobFile mutateInstance(BlobFile instance) throws IOException {
        var mutatedTermAndGen = mutatePrimaryTermAndGeneration(instance.termAndGeneration());
        return new BccBlobFile(mutatedTermAndGen);
    }

    public void testValidation() {
        expectThrows(AssertionError.class, () -> new BccBlobFile(randomAlphaOfLengthBetween(0, 30), randomPrimaryTermAndGeneration()));
    }
}
