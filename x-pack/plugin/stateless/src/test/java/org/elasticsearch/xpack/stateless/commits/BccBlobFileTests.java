/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomNonZeroPositiveLong;
import static org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests.mutatePrimaryTermAndGeneration;
import static org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests.randomPrimaryTermAndGeneration;
import static org.hamcrest.Matchers.equalTo;

public class BccBlobFileTests extends AbstractWireSerializingTestCase<BlobFile> {
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
        assumeTrue("the validation is an assertion", Assertions.ENABLED);
        expectThrows(AssertionError.class, () -> new BccBlobFile(randomAlphaOfLengthBetween(0, 30), randomPrimaryTermAndGeneration()));
    }

    public void testBlobNameFromGeneration() {
        final long generation = randomNonZeroPositiveLong();
        assertThat(BccBlobFile.blobNameFromGeneration(generation), equalTo("stateless_commit_" + generation));
        assertThat(BccBlobFile.parseGenerationFromBlobName(BccBlobFile.blobNameFromGeneration(generation)), equalTo(generation));
    }

    public void testGenerationMustBePositive() {
        assumeTrue("the validation is an assertion", Assertions.ENABLED);
        expectThrows(AssertionError.class, () -> BccBlobFile.blobNameFromGeneration(0L));
        expectThrows(AssertionError.class, () -> BccBlobFile.blobNameFromGeneration(-1L));
    }

    public void testBlobNameDoesNotLookLikeAMergeBlobName() {
        final var blob = new BccBlobFile(randomPrimaryTermAndGeneration());
        assertTrue(BccBlobFile.startsWithBlobPrefix(blob.blobName()));
        assertFalse(MergeBlobFile.startsWithBlobPrefix(blob.blobName()));
    }

    public void testCreateFromBlobNameAndPrimaryTerm() {
        final var termAndGen = randomPrimaryTermAndGeneration();
        final var blob = new BccBlobFile(BccBlobFile.blobNameFromGeneration(termAndGen.generation()), termAndGen.primaryTerm());
        assertThat(blob, equalTo(new BccBlobFile(termAndGen)));
        assertThat(blob.generation(), equalTo(termAndGen.generation()));
        assertThat(blob.primaryTerm(), equalTo(termAndGen.primaryTerm()));
    }
}
