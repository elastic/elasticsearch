/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.commits.BlobFileTestUtils.randomMergeBlobFile;
import static org.elasticsearch.xpack.stateless.commits.BlobFileTestUtils.randomMergeDiscriminator;
import static org.elasticsearch.xpack.stateless.commits.BlobFileTestUtils.randomMergeSegment;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomNonZeroPositiveLong;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class MergeBlobFileTests extends AbstractWireSerializingTestCase<BlobFile> {

    @Override
    protected Writeable.Reader<BlobFile> instanceReader() {
        return BlobFile::readFrom;
    }

    @Override
    protected BlobFile createTestInstance() {
        return randomMergeBlobFile();
    }

    @Override
    protected BlobFile mutateInstance(BlobFile instance) throws IOException {
        var blob = (MergeBlobFile) instance;
        long primaryTerm = blob.primaryTerm();
        long generation = blob.mergeStartGeneration();
        String segment = blob.segment();
        String discriminator = blob.discriminator();
        switch (randomIntBetween(0, 3)) {
            case 0 -> primaryTerm = randomValueOtherThan(primaryTerm, StatelessCompoundCommitTestUtils::randomNonZeroPositiveLong);
            case 1 -> generation = randomValueOtherThan(generation, StatelessCompoundCommitTestUtils::randomNonZeroPositiveLong);
            case 2 -> segment = randomValueOtherThan(segment, () -> randomMergeSegment());
            case 3 -> discriminator = randomValueOtherThan(discriminator, () -> randomMergeDiscriminator());
            default -> throw new AssertionError("unreachable");
        }
        return new MergeBlobFile(MergeBlobFile.blobName(generation, segment, discriminator), primaryTerm);
    }

    public void testAccessors() {
        final long primaryTerm = randomNonZeroPositiveLong();
        final long generation = randomNonZeroPositiveLong();
        final String segment = randomMergeSegment();
        final String discriminator = randomMergeDiscriminator();

        final var blob = new MergeBlobFile(MergeBlobFile.blobName(generation, segment, discriminator), primaryTerm);

        assertThat(blob.blobName(), equalTo("stateless_merge_" + generation + '_' + segment + '_' + discriminator));
        assertThat(blob.primaryTerm(), equalTo(primaryTerm));
        assertThat(blob.mergeStartGeneration(), equalTo(generation));
        assertThat(blob.segment(), equalTo(segment));
        assertThat(blob.discriminator(), equalTo(discriminator));
        assertThat(blob.termAndGeneration(), equalTo(new PrimaryTermAndGeneration(primaryTerm, generation)));
    }

    public void testExampleFromDesign() {
        final var blob = new MergeBlobFile("stateless_merge_42_1b_9f3a1c7e5b2d4068", 3L);
        assertThat(blob.mergeStartGeneration(), equalTo(42L));
        assertThat(blob.segment(), equalTo("1b"));
        assertThat(blob.discriminator(), equalTo("9f3a1c7e5b2d4068"));
    }

    public void testBlobNameDoesNotLookLikeABccBlobName() {
        final var blob = randomMergeBlobFile();
        assertTrue(MergeBlobFile.startsWithBlobPrefix(blob.blobName()));
        assertFalse(BccBlobFile.startsWithBlobPrefix(blob.blobName()));
    }

    public void testBlobFileOfDeducesTheKindFromTheName() {
        final var merge = randomMergeBlobFile();
        assertThat(BlobFile.of(merge.primaryTerm(), merge.blobName()), equalTo(merge));

        final var bccTermAndGen = new PrimaryTermAndGeneration(randomNonZeroPositiveLong(), randomNonZeroPositiveLong());
        final var bcc = new BccBlobFile(bccTermAndGen);
        assertThat(BlobFile.of(bcc.primaryTerm(), bcc.blobName()), instanceOf(BccBlobFile.class));
        assertThat(BlobFile.of(bcc.primaryTerm(), bcc.blobName()), equalTo(bcc));
    }

    public void testBlobFileOfRejectsUnknownNames() {
        expectThrows(IllegalArgumentException.class, () -> BlobFile.of(randomNonZeroPositiveLong(), "stateless_other_1"));
    }

    public void testValidation() {
        final String segment = randomMergeSegment();
        final String discriminator = randomMergeDiscriminator();
        final long generation = randomNonZeroPositiveLong();
        final long primaryTerm = randomNonZeroPositiveLong();

        // not a merge blob name
        expectThrows(IllegalArgumentException.class, () -> new MergeBlobFile(BccBlobFile.PREFIX + generation, primaryTerm));
        expectThrows(IllegalArgumentException.class, () -> new MergeBlobFile(randomAlphaOfLengthBetween(0, 30), primaryTerm));

        // missing parts
        expectThrows(IllegalArgumentException.class, () -> new MergeBlobFile(MergeBlobFile.PREFIX, primaryTerm));
        expectThrows(IllegalArgumentException.class, () -> new MergeBlobFile(MergeBlobFile.PREFIX + generation, primaryTerm));
        expectThrows(
            IllegalArgumentException.class,
            () -> new MergeBlobFile(MergeBlobFile.PREFIX + generation + '_' + segment, primaryTerm)
        );

        // invalid generation
        expectThrows(
            IllegalArgumentException.class,
            () -> new MergeBlobFile(MergeBlobFile.PREFIX + "0_" + segment + '_' + discriminator, primaryTerm)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new MergeBlobFile(MergeBlobFile.PREFIX + "-1_" + segment + '_' + discriminator, primaryTerm)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new MergeBlobFile(MergeBlobFile.PREFIX + "abc_" + segment + '_' + discriminator, primaryTerm)
        );

        // invalid segment
        expectThrows(
            IllegalArgumentException.class,
            () -> new MergeBlobFile(MergeBlobFile.PREFIX + generation + "__" + discriminator, primaryTerm)
        );

        // invalid discriminator
        expectThrows(
            IllegalArgumentException.class,
            () -> new MergeBlobFile(MergeBlobFile.PREFIX + generation + '_' + segment + '_' + discriminator.substring(1), primaryTerm)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new MergeBlobFile(MergeBlobFile.PREFIX + generation + '_' + segment + '_' + "ABCDEF0123456789", primaryTerm)
        );
    }

    public void testBlobNameRejectsInvalidParts() {
        expectThrows(IllegalArgumentException.class, () -> MergeBlobFile.blobName(1L, "", randomMergeDiscriminator()));
        expectThrows(IllegalArgumentException.class, () -> MergeBlobFile.blobName(1L, "a_b", randomMergeDiscriminator()));
        expectThrows(IllegalArgumentException.class, () -> MergeBlobFile.blobName(1L, randomMergeSegment(), "short"));
        expectThrows(IllegalArgumentException.class, () -> MergeBlobFile.blobName(1L, randomMergeSegment(), "ZZZZZZZZZZZZZZZZ"));
    }

    public void testDeserializationFailsOnGenerationMismatch() throws IOException {
        final var blob = randomMergeBlobFile();
        try (var out = new BytesStreamOutput()) {
            out.writeString(blob.blobName());
            new PrimaryTermAndGeneration(blob.primaryTerm(), blob.mergeStartGeneration() + 1).writeTo(out);
            expectThrows(IOException.class, () -> BlobFile.readFrom(out.bytes().streamInput()));
        }
    }
}
