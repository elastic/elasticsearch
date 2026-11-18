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
        return switch (between(0, 1)) {
            case 0 -> new MergeBlob(instance.blobName(), randomValueOtherThan(instance.primaryTerm(), () -> randomNonNegativeLong()));
            case 1 -> new MergeBlob(
                randomValueOtherThan(instance.blobName(), () -> MergeBlob.blobName(randomSegmentId())),
                instance.primaryTerm()
            );
            default -> throw new AssertionError();
        };
    }

    public void testNameParts() {
        var segmentId = randomSegmentId();
        var primaryTerm = randomNonNegativeLong();

        var blobName = MergeBlob.blobName(segmentId);
        assertThat(blobName, equalTo("stateless_merge_" + HexFormat.of().formatHex(segmentId)));

        var blob = new MergeBlob(blobName, primaryTerm);
        assertThat(blob.blobName(), equalTo(blobName));
        assertThat(blob.primaryTerm(), equalTo(primaryTerm));
        assertThat(blob.segmentId(), equalTo(HexFormat.of().formatHex(segmentId)));
        assertThat(blob.segmentId().length(), equalTo(2 * StringHelper.ID_LENGTH));
        assertThat(blob.toString(), containsString(blobName));
    }

    public void testStartsWithPrefix() {
        assertTrue(MergeBlob.startsWithPrefix(MergeBlob.PREFIX));
        assertTrue(MergeBlob.startsWithPrefix(MergeBlob.blobName(randomSegmentId())));
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

    public void testRejectsInvalidSegmentId() {
        var valid = HexFormat.of().formatHex(randomSegmentId());
        for (var invalid : new String[] {
            "",
            valid.substring(1),
            valid + randomFrom("0", "a"),
            // always contains at least one uppercase letter, even if the id only has digits
            'A' + valid.substring(1).toUpperCase(Locale.ROOT),
            valid.substring(1) + 'g',
            valid.substring(1) + '_',
            // the former structure of merge blob names, with a generation, a segment and a discriminator
            randomNonZeroPositiveLong() + "_1b_" + HexFormat.of().toHexDigits(randomLong()) }) {
            var blobName = MergeBlob.PREFIX + invalid;
            var exception = expectThrows(IllegalArgumentException.class, blobName, () -> new MergeBlob(blobName, randomNonNegativeLong()));
            assertThat(exception.getMessage(), containsString("invalid segment id"));
        }
    }

    public void testBlobNameRejectsInvalidSegmentIdLength() {
        var length = randomValueOtherThan(StringHelper.ID_LENGTH, () -> between(0, 2 * StringHelper.ID_LENGTH));
        var exception = expectThrows(IllegalArgumentException.class, () -> MergeBlob.blobName(randomByteArrayOfLength(length)));
        assertThat(exception.getMessage(), containsString("invalid segment id length [" + length + ']'));
    }

    /// A merge blob is read back as a [MergeBlob] from the stream, whatever the expected type is as long as it is compatible.
    public void testReadAsBlob() throws IOException {
        var blob = randomMergeBlob();
        var read = copyWriteable(blob, writableRegistry(), Blob.reader(Blob.class), TransportVersionUtils.randomCompatibleVersion());
        assertThat(read, equalTo(blob));
        assertThat(read, instanceOf(MergeBlob.class));
    }

    /// Reading a [BlobFile] where a [MergeBlob] is expected must fail cleanly. The opposite case is tested in [BlobFileTests].
    public void testReadRejectsBlobFileName() {
        var blobFile = new BlobFile(BatchedCompoundCommit.blobNameFromGeneration(randomNonZeroPositiveLong()), randomNonNegativeLong());
        Writeable.Reader<Writeable> reader = in -> Blob.reader(MergeBlob.class).read(in);
        var exception = expectThrows(
            IOException.class,
            () -> copyWriteable(blobFile, writableRegistry(), reader, TransportVersionUtils.randomCompatibleVersion())
        );
        assertThat(exception.getMessage(), containsString("expected a [MergeBlob] but blob [" + blobFile.blobName() + "] is a [BlobFile]"));
    }

    public void testReadRejectsMalformedBlobName() {
        var blobName = MergeBlob.PREFIX + "not-a-segment-id";
        Writeable invalidBlob = out -> {
            out.writeString(blobName);
            out.writeVLong(randomNonNegativeLong());
        };
        Writeable.Reader<Writeable> reader = in -> Blob.reader(MergeBlob.class).read(in);
        var exception = expectThrows(
            IOException.class,
            () -> copyWriteable(invalidBlob, writableRegistry(), reader, TransportVersionUtils.randomCompatibleVersion())
        );
        assertThat(exception.getMessage(), startsWith("malformed blob name [" + blobName + ']'));
    }

    private static MergeBlob randomMergeBlob() {
        return new MergeBlob(MergeBlob.blobName(randomSegmentId()), randomNonNegativeLong());
    }

    private static byte[] randomSegmentId() {
        return randomByteArrayOfLength(StringHelper.ID_LENGTH);
    }
}
