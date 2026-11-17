/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.HexFormat;
import java.util.Locale;

import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomNonZeroPositiveLong;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class MergeBlobTests extends AbstractWireSerializingTestCase<MergeBlob> {

    @Override
    protected Writeable.Reader<MergeBlob> instanceReader() {
        return Blob.reader(MergeBlob.class);
    }

    @Override
    protected MergeBlob createTestInstance() {
        return randomMergeBlob();
    }

    @Override
    protected MergeBlob mutateInstance(MergeBlob instance) throws IOException {
        return switch (between(0, 3)) {
            case 0 -> new MergeBlob(instance.blobName(), randomValueOtherThan(instance.primaryTerm(), () -> randomNonNegativeLong()));
            case 1 -> new MergeBlob(
                MergeBlob.blobName(
                    randomValueOtherThan(instance.mergeStartGeneration(), () -> randomNonZeroPositiveLong()),
                    instance.segment(),
                    instance.discriminator()
                ),
                instance.primaryTerm()
            );
            case 2 -> new MergeBlob(
                MergeBlob.blobName(
                    instance.mergeStartGeneration(),
                    randomValueOtherThan(instance.segment(), () -> randomSegment()),
                    instance.discriminator()
                ),
                instance.primaryTerm()
            );
            case 3 -> new MergeBlob(
                MergeBlob.blobName(
                    instance.mergeStartGeneration(),
                    instance.segment(),
                    randomValueOtherThan(instance.discriminator(), () -> randomDiscriminator())
                ),
                instance.primaryTerm()
            );
            default -> throw new AssertionError();
        };
    }

    public void testNameParts() {
        var generation = randomNonZeroPositiveLong();
        var segment = randomSegment();
        var discriminator = randomDiscriminator();
        var primaryTerm = randomNonNegativeLong();

        var blobName = MergeBlob.blobName(generation, segment, discriminator);
        assertThat(blobName, equalTo("stateless_merge_" + generation + '_' + segment + '_' + discriminator));

        var blob = new MergeBlob(blobName, primaryTerm);
        assertThat(blob.blobName(), equalTo(blobName));
        assertThat(blob.primaryTerm(), equalTo(primaryTerm));
        assertThat(blob.mergeStartGeneration(), equalTo(generation));
        assertThat(blob.segment(), equalTo(segment));
        assertThat(blob.discriminator(), equalTo(discriminator));
        assertThat(blob.termAndGeneration(), equalTo(new PrimaryTermAndGeneration(primaryTerm, generation)));
        assertThat(blob.toString(), containsString(blobName));
    }

    public void testStartsWithPrefix() {
        assertTrue(MergeBlob.startsWithPrefix(MergeBlob.PREFIX));
        assertTrue(MergeBlob.startsWithPrefix(MergeBlob.blobName(randomNonZeroPositiveLong(), randomSegment(), randomDiscriminator())));
        assertFalse(MergeBlob.startsWithPrefix(BatchedCompoundCommit.blobNameFromGeneration(randomNonZeroPositiveLong())));
        assertFalse(MergeBlob.startsWithPrefix("foo_" + randomNonNegativeInt()));
        assertFalse(MergeBlob.startsWithPrefix(""));
    }

    public void testRejectsNameWithoutPrefix() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new MergeBlob(BatchedCompoundCommit.blobNameFromGeneration(randomNonZeroPositiveLong()), randomNonNegativeLong())
        );
        assertThat(exception.getMessage(), containsString("not a merge blob name"));
    }

    public void testRejectsNonNumericGeneration() {
        var blobName = MergeBlob.PREFIX + randomAlphaOfLength(3).toLowerCase(Locale.ROOT) + "_1b_" + randomDiscriminator();
        var exception = expectThrows(IllegalArgumentException.class, () -> new MergeBlob(blobName, randomNonNegativeLong()));
        assertThat(exception, instanceOf(NumberFormatException.class));
    }

    public void testRejectsNonPositiveGeneration() {
        var generation = randomFrom("0", "-1", "-" + randomLongBetween(1, Long.MAX_VALUE));
        var blobName = MergeBlob.PREFIX + generation + "_1b_" + randomDiscriminator();
        var exception = expectThrows(IllegalArgumentException.class, () -> new MergeBlob(blobName, randomNonNegativeLong()));
        assertThat(exception.getMessage(), containsString("invalid merge start generation"));
    }

    public void testRejectsMissingParts() {
        for (var suffix : new String[] { "", "5", "5_", "5_1b", "5_1b_", "5__" + randomDiscriminator(), "5_" + randomDiscriminator() }) {
            var blobName = MergeBlob.PREFIX + suffix;
            expectThrows(IllegalArgumentException.class, blobName, () -> new MergeBlob(blobName, randomNonNegativeLong()));
        }
    }

    public void testRejectsInvalidSegment() {
        var blobName = MergeBlob.PREFIX + randomNonZeroPositiveLong() + "_1_b_" + randomDiscriminator();
        var exception = expectThrows(IllegalArgumentException.class, () -> new MergeBlob(blobName, randomNonNegativeLong()));
        assertThat(exception.getMessage(), containsString("invalid"));

        expectThrows(
            IllegalArgumentException.class,
            () -> MergeBlob.blobName(randomNonZeroPositiveLong(), randomFrom("", "a_b", "_a"), randomDiscriminator())
        );
    }

    public void testRejectsInvalidDiscriminator() {
        var generation = randomNonZeroPositiveLong();
        var invalid = randomFrom(
            "",
            randomAlphaOfLength(15).toLowerCase(Locale.ROOT).replace('g', 'a'),
            "0123456789abcdef0",
            "0123456789ABCDEF",
            "0123456789abcdeg"
        );
        var blobName = MergeBlob.PREFIX + generation + "_1b_" + invalid;
        var exception = expectThrows(IllegalArgumentException.class, () -> new MergeBlob(blobName, randomNonNegativeLong()));
        assertThat(exception.getMessage(), containsString("invalid discriminator"));

        expectThrows(IllegalArgumentException.class, () -> MergeBlob.blobName(generation, "1b", invalid));
    }

    public void testBlobNameRejectsNonPositiveGeneration() {
        expectThrows(
            IllegalArgumentException.class,
            () -> MergeBlob.blobName(randomFrom(0L, -randomLongBetween(1, Long.MAX_VALUE)), randomSegment(), randomDiscriminator())
        );
    }

    public void testParseGenerationFromBlobName() {
        var generation = randomNonZeroPositiveLong();
        var blobName = MergeBlob.blobName(generation, randomSegment(), randomDiscriminator());
        assertThat(MergeBlob.parseGenerationFromBlobName(blobName), equalTo(generation));
    }

    /// A merge blob is read back as a [MergeBlob] from the stream, whatever the expected type is as long as it is compatible.
    public void testReadAsBlob() throws IOException {
        var blob = randomMergeBlob();
        var read = copyWriteable(blob, writableRegistry(), Blob.reader(Blob.class), TransportVersionUtils.randomCompatibleVersion());
        assertThat(read, equalTo(blob));
        assertThat(read, instanceOf(MergeBlob.class));
    }

    public void testReadRejectsBlobFileExpectation() {
        var blob = randomMergeBlob();
        Writeable.Reader<Writeable> reader = in -> Blob.reader(BlobFile.class).read(in);
        var exception = expectThrows(
            IOException.class,
            () -> copyWriteable(blob, writableRegistry(), reader, TransportVersionUtils.randomCompatibleVersion())
        );
        assertThat(exception.getMessage(), containsString("expected a [BlobFile] but blob [" + blob.blobName() + "] is a [MergeBlob]"));
    }

    public void testReadRejectsMalformedBlobName() {
        var termAndGen = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomNonZeroPositiveLong());
        var blobName = MergeBlob.PREFIX + termAndGen.generation() + "_1b_" + "not-a-discriminator";
        var exception = expectThrows(IOException.class, () -> readInvalidBlob(blobName, termAndGen));
        assertThat(exception.getMessage(), startsWith("malformed blob name [" + blobName + ']'));
    }

    public void testReadRejectsTermAndGenerationMismatch() {
        var termAndGen = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomNonZeroPositiveLong());
        var otherGeneration = randomValueOtherThan(termAndGen.generation(), () -> randomNonZeroPositiveLong());
        var blobName = MergeBlob.blobName(otherGeneration, randomSegment(), randomDiscriminator());
        var exception = expectThrows(IOException.class, () -> readInvalidBlob(blobName, termAndGen));
        assertThat(exception.getMessage(), containsString("term/generation mismatch"));
    }

    /// Writes the given blob name and term/generation as is, which a [MergeBlob] cannot do for invalid values, and reads them back through
    /// [Blob#reader(Class)] with a random transport version.
    private void readInvalidBlob(String blobName, PrimaryTermAndGeneration termAndGeneration) throws IOException {
        Writeable invalidBlob = out -> {
            out.writeString(blobName);
            termAndGeneration.writeTo(out);
        };
        Writeable.Reader<Writeable> reader = in -> Blob.reader(MergeBlob.class).read(in);
        copyWriteable(invalidBlob, writableRegistry(), reader, TransportVersionUtils.randomCompatibleVersion());
    }

    private static MergeBlob randomMergeBlob() {
        return new MergeBlob(
            MergeBlob.blobName(randomNonZeroPositiveLong(), randomSegment(), randomDiscriminator()),
            randomNonNegativeLong()
        );
    }

    private static String randomSegment() {
        return randomAlphanumericOfLength(between(1, 8)).toLowerCase(Locale.ROOT);
    }

    private static String randomDiscriminator() {
        return HexFormat.of().toHexDigits(randomLong());
    }
}
