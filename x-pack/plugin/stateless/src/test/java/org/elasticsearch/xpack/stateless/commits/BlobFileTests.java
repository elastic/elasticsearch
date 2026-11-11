/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit.blobNameFromGeneration;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomNonZeroPositiveLong;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

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
        var termAndGen = randomTermAndGeneration();
        return new BlobFile(blobNameFromGeneration(termAndGen.generation()), termAndGen);
    }

    @Override
    protected BlobFile mutateInstance(BlobFile instance) throws IOException {
        var mutatedTermAndGen = mutateTermAndGeneration(instance.termAndGeneration());
        return new BlobFile(blobNameFromGeneration(mutatedTermAndGen.generation()), mutatedTermAndGen);
    }

    public void testRejectsUnrecognizedBlobName() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new BlobFile("foo_" + randomNonNegativeInt(), randomTermAndGeneration())
        );
        assertThat(exception.getMessage(), containsString("unrecognized blob name"));
    }

    public void testRejectsMergeBlobName() {
        var blobName = "stateless_merge_" + randomNonNegativeInt() + "_1b_9f3a1c7e5b2d4068";
        var exception = expectThrows(IllegalArgumentException.class, () -> new BlobFile(blobName, randomTermAndGeneration()));
        assertThat(exception.getMessage(), containsString("unrecognized blob name"));
    }

    public void testRejectsNonNumericGeneration() {
        var blobName = BatchedCompoundCommit.PREFIX + randomAlphaOfLengthBetween(1, 10);
        var exception = expectThrows(IllegalArgumentException.class, () -> new BlobFile(blobName, randomTermAndGeneration()));
        assertThat(exception.getMessage(), containsString("malformed blob name [" + blobName + ']'));
        assertThat(exception.getCause(), instanceOf(NumberFormatException.class));
    }

    public void testGenerationIsDeducedFromBlobName() {
        var primaryTerm = randomNonNegativeLong();
        var generation = randomLongBetween(1, Long.MAX_VALUE);
        var blobFile = new BlobFile(blobNameFromGeneration(generation), primaryTerm);
        assertEquals(new PrimaryTermAndGeneration(primaryTerm, generation), blobFile.termAndGeneration());
    }

    public void testRejectsInvalidBlobNameWithPrimaryTerm() {
        var unrecognized = expectThrows(
            IllegalArgumentException.class,
            () -> new BlobFile("foo_" + randomNonNegativeInt(), randomNonNegativeLong())
        );
        assertThat(unrecognized.getMessage(), containsString("unrecognized blob name"));

        var malformed = expectThrows(
            IllegalArgumentException.class,
            () -> new BlobFile(BatchedCompoundCommit.PREFIX + randomAlphaOfLengthBetween(1, 10), randomNonNegativeLong())
        );
        assertThat(malformed.getMessage(), containsString("malformed blob name"));
        assertThat(malformed.getCause(), instanceOf(NumberFormatException.class));
    }

    public void testRejectsNonPositiveGeneration() {
        var blobName = BatchedCompoundCommit.PREFIX + randomFrom("0", "-1", "-" + randomLongBetween(1, Long.MAX_VALUE));
        var generation = Long.parseLong(blobName.substring(BatchedCompoundCommit.PREFIX.length()));

        var fromBlobName = expectThrows(IllegalArgumentException.class, () -> new BlobFile(blobName, randomNonNegativeLong()));
        assertThat(fromBlobName.getMessage(), containsString("the generation must be positive"));

        var fromTermAndGeneration = expectThrows(
            IllegalArgumentException.class,
            () -> new BlobFile(blobName, new PrimaryTermAndGeneration(randomNonNegativeLong(), generation))
        );
        assertThat(fromTermAndGeneration.getMessage(), containsString("the generation must be positive"));
    }

    public void testRejectsGenerationMismatch() {
        var termAndGen = randomTermAndGeneration();
        var blobName = blobNameFromGeneration(randomValueOtherThan(termAndGen.generation(), () -> randomNonZeroPositiveLong()));
        var exception = expectThrows(IllegalArgumentException.class, () -> new BlobFile(blobName, termAndGen));
        assertThat(exception.getMessage(), containsString("generation mismatch"));
    }

    /// Unlike [org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests#randomPrimaryTermAndGeneration()], the generation is
    /// always positive, as a [BlobFile] cannot have a generation of 0.
    private static PrimaryTermAndGeneration randomTermAndGeneration() {
        return new PrimaryTermAndGeneration(randomNonNegativeLong(), randomNonZeroPositiveLong());
    }

    private static PrimaryTermAndGeneration mutateTermAndGeneration(PrimaryTermAndGeneration instance) {
        if (randomBoolean()) {
            return new PrimaryTermAndGeneration(
                randomValueOtherThan(instance.primaryTerm(), () -> randomNonNegativeLong()),
                instance.generation()
            );
        }
        return new PrimaryTermAndGeneration(
            instance.primaryTerm(),
            randomValueOtherThan(instance.generation(), () -> randomNonZeroPositiveLong())
        );
    }
}
