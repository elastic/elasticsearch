/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.commits.BccBlob.blobNameFromGeneration;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomNonZeroPositiveLong;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class BccBlobTests extends AbstractWireSerializingTestCase<BccBlob> {
    @Override
    protected Writeable.Reader<BccBlob> instanceReader() {
        return Blob.reader(BccBlob.class);
    }

    @Override
    protected BccBlob createTestInstance() {
        return randomBccBlob();
    }

    private BccBlob randomBccBlob() {
        var termAndGen = randomTermAndGeneration();
        return new BccBlob(blobNameFromGeneration(termAndGen.generation()), termAndGen);
    }

    @Override
    protected BccBlob mutateInstance(BccBlob instance) throws IOException {
        var mutatedTermAndGen = mutateTermAndGeneration(instance.termAndGeneration());
        return new BccBlob(blobNameFromGeneration(mutatedTermAndGen.generation()), mutatedTermAndGen);
    }

    public void testRejectsUnrecognizedBlobName() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new BccBlob("foo_" + randomNonNegativeInt(), randomTermAndGeneration())
        );
        assertThat(exception.getMessage(), containsString("unrecognized blob name"));
    }

    public void testRejectsMergeBlobName() {
        var blobName = MergeBlob.blobName(randomByteArrayOfLength(StringHelper.ID_LENGTH));
        var exception = expectThrows(IllegalArgumentException.class, () -> new BccBlob(blobName, randomTermAndGeneration()));
        assertThat(exception.getMessage(), containsString("unrecognized blob name"));
    }

    public void testRejectsNonNumericGeneration() {
        var blobName = BccBlob.PREFIX + randomAlphaOfLengthBetween(1, 10);
        var exception = expectThrows(IllegalArgumentException.class, () -> new BccBlob(blobName, randomTermAndGeneration()));
        assertThat(exception.getMessage(), containsString("malformed blob name [" + blobName + ']'));
        assertThat(exception.getCause(), instanceOf(NumberFormatException.class));
    }

    public void testGenerationIsDeducedFromBlobName() {
        var primaryTerm = randomNonNegativeLong();
        var generation = randomLongBetween(1, Long.MAX_VALUE);
        var blobFile = new BccBlob(blobNameFromGeneration(generation), primaryTerm);
        assertEquals(new PrimaryTermAndGeneration(primaryTerm, generation), blobFile.termAndGeneration());
    }

    public void testRejectsInvalidBlobNameWithPrimaryTerm() {
        var unrecognized = expectThrows(
            IllegalArgumentException.class,
            () -> new BccBlob("foo_" + randomNonNegativeInt(), randomNonNegativeLong())
        );
        assertThat(unrecognized.getMessage(), containsString("unrecognized blob name"));

        var malformed = expectThrows(
            IllegalArgumentException.class,
            () -> new BccBlob(BccBlob.PREFIX + randomAlphaOfLengthBetween(1, 10), randomNonNegativeLong())
        );
        assertThat(malformed.getMessage(), containsString("malformed blob name"));
        assertThat(malformed.getCause(), instanceOf(NumberFormatException.class));
    }

    public void testRejectsNonPositiveGeneration() {
        var blobName = BccBlob.PREFIX + randomFrom("0", "-1", "-" + randomLongBetween(1, Long.MAX_VALUE));
        var generation = Long.parseLong(blobName.substring(BccBlob.PREFIX.length()));

        var fromBlobName = expectThrows(IllegalArgumentException.class, () -> new BccBlob(blobName, randomNonNegativeLong()));
        assertThat(fromBlobName.getMessage(), containsString("the generation must be positive"));

        var fromTermAndGeneration = expectThrows(
            IllegalArgumentException.class,
            () -> new BccBlob(blobName, new PrimaryTermAndGeneration(randomNonNegativeLong(), generation))
        );
        assertThat(fromTermAndGeneration.getMessage(), containsString("the generation must be positive"));
    }

    public void testRejectsGenerationMismatch() {
        var termAndGen = randomTermAndGeneration();
        var blobName = blobNameFromGeneration(randomValueOtherThan(termAndGen.generation(), () -> randomNonZeroPositiveLong()));
        var exception = expectThrows(IllegalArgumentException.class, () -> new BccBlob(blobName, termAndGen));
        assertThat(exception.getMessage(), containsString("generation mismatch"));
    }

    public void testReadRejectsUnrecognizedBlobName() {
        var exception = expectThrows(IOException.class, () -> readInvalidBlob("foo_" + randomNonNegativeInt(), randomTermAndGeneration()));
        assertThat(exception.getMessage(), containsString("unrecognized blob name"));
    }

    /// Reading a merge blob where a [BccBlob] is expected must fail cleanly rather than be mistaken for a batched compound commit.
    public void testReadRejectsMergeBlobName() {
        var mergeBlob = new MergeBlob(MergeBlob.blobName(randomByteArrayOfLength(StringHelper.ID_LENGTH)), randomNonNegativeLong());
        Writeable.Reader<Writeable> reader = in -> Blob.reader(BccBlob.class).read(in);
        var exception = expectThrows(
            IOException.class,
            () -> copyWriteable(mergeBlob, writableRegistry(), reader, TransportVersionUtils.randomCompatibleVersion())
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "expected a ["
                    + BccBlob.class.getSimpleName()
                    + "] but blob ["
                    + mergeBlob.blobName()
                    + "] is a ["
                    + mergeBlob.getClass().getSimpleName()
                    + ']'
            )
        );
    }

    public void testReadRejectsMalformedGeneration() {
        var blobName = BccBlob.PREFIX + randomAlphaOfLengthBetween(1, 10);
        var exception = expectThrows(IOException.class, () -> readInvalidBlob(blobName, randomTermAndGeneration()));
        assertThat(exception.getMessage(), containsString("malformed blob name"));
    }

    public void testReadRejectsTermAndGenerationMismatch() {
        var termAndGen = randomTermAndGeneration();
        var otherGeneration = termAndGen.generation() + randomLongBetween(1, 100);
        var blobName = blobNameFromGeneration(otherGeneration);
        var exception = expectThrows(IOException.class, () -> readInvalidBlob(blobName, termAndGen));
        assertThat(exception.getMessage(), containsString("term/generation mismatch"));
    }

    /// Writes the given blob name and term/generation as is, which a [BccBlob] cannot do for invalid values, and reads them back through
    /// [Blob#reader(Class)] with a random transport version.
    private void readInvalidBlob(String blobName, PrimaryTermAndGeneration termAndGeneration) throws IOException {
        Writeable invalidBlob = out -> {
            out.writeString(blobName);
            termAndGeneration.writeTo(out);
        };
        Writeable.Reader<Writeable> reader = in -> Blob.reader(BccBlob.class).read(in);
        copyWriteable(invalidBlob, writableRegistry(), reader, TransportVersionUtils.randomCompatibleVersion());
    }

    /// Unlike [org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests#randomPrimaryTermAndGeneration()], the generation is
    /// always positive, as a [BccBlob] cannot have a generation of 0.
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
