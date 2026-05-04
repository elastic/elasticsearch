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

import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.blobNameFromGeneration;
import static org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests.mutatePrimaryTermAndGeneration;
import static org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests.randomPrimaryTermAndGeneration;

public class BlobFileTests extends AbstractWireSerializingTestCase<BlobFile> {
    @Override
    protected Writeable.Reader<BlobFile> instanceReader() {
        return BlobFile::new;
    }

    @Override
    protected BlobFile createTestInstance() {
        return randomBlobFile();
    }

    private BlobFile randomBlobFile() {
        var termAndGen = randomPrimaryTermAndGeneration();
        return new BlobFile(blobNameFromGeneration(termAndGen.generation()), termAndGen);
    }

    @Override
    protected BlobFile mutateInstance(BlobFile instance) throws IOException {
        var mutatedTermAndGen = mutatePrimaryTermAndGeneration(instance.termAndGeneration());
        return new BlobFile(blobNameFromGeneration(mutatedTermAndGen.generation()), mutatedTermAndGen);
    }

    public void testValidation() {
        expectThrows(AssertionError.class, () -> new BlobFile(randomAlphaOfLengthBetween(0, 30), randomPrimaryTermAndGeneration()));
    }
}
