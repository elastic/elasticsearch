/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.lucene.util.StringHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.stateless.commits.BlobLocation.BLOB_LOCATION_SERIALIZES_BLOB;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class BlobLocationTests extends AbstractXContentSerializingTestCase<BlobLocation> {

    @Override
    protected Writeable.Reader<BlobLocation> instanceReader() {
        return BlobLocation::readFromTransport;
    }

    @Override
    protected BlobLocation createTestInstance() {
        return new BlobLocation(randomBlob(), randomLongBetween(0, 100), randomLongBetween(100, 1000));
    }

    @Override
    protected BlobLocation mutateInstance(BlobLocation instance) throws IOException {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new BlobLocation(
                randomValueOtherThan(instance.blob(), BlobLocationTests::randomBlob),
                instance.offset(),
                instance.fileLength()
            );
            case 1 -> new BlobLocation(
                instance.blob(),
                randomValueOtherThan(instance.offset(), () -> randomLongBetween(0, 100)),
                instance.fileLength()
            );
            case 2 -> new BlobLocation(
                instance.blob(),
                instance.offset(),
                randomValueOtherThan(instance.fileLength(), () -> randomLongBetween(100, 1000))
            );
            default -> throw new AssertionError();
        };
    }

    @Override
    protected BlobLocation doParseInstance(XContentParser parser) throws IOException {
        return BlobLocation.fromXContent(parser);
    }

    /// With the current transport version, the blob is written with [Blob#writeTo], followed by the offset and the length.
    public void testWriteToUsesBlobWriteTo() throws IOException {
        var location = createTestInstance();

        var expected = new BytesStreamOutput();
        expected.setTransportVersion(TransportVersion.current());
        location.blob().writeTo(expected);
        expected.writeVLong(location.offset());
        expected.writeVLong(location.fileLength());

        var actual = new BytesStreamOutput();
        actual.setTransportVersion(TransportVersion.current());
        location.writeTo(actual);

        assertThat(actual.bytes(), equalTo(expected.bytes()));
    }

    /// Both kinds of blob survive a round trip on the transport layer, and are read back as the same type.
    public void testTransportRoundTripOfEachKindOfBlob() throws IOException {
        for (Blob blob : List.of(randomBccBlob(), randomMergeBlob())) {
            var location = new BlobLocation(blob, randomLongBetween(0, 100), randomLongBetween(1, 1000));
            var copy = copyWriteable(location, writableRegistry(), BlobLocation::readFromTransport, TransportVersion.current());
            assertThat(copy, equalTo(location));
            assertThat(copy.blob().getClass(), equalTo(blob.getClass()));
        }
    }

    /// Nodes of previous versions read a primary term followed by a blob name, so that is what is written to them.
    public void testWireFormatBeforeBlobSerialization() throws IOException {
        var oldVersion = TransportVersionUtils.getPreviousVersion(BLOB_LOCATION_SERIALIZES_BLOB);
        var location = new BlobLocation(randomBccBlob(), randomLongBetween(0, 100), randomLongBetween(1, 1000));

        var out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        location.writeTo(out);

        var in = out.bytes().streamInput();
        in.setTransportVersion(oldVersion);
        assertThat(in.readVLong(), equalTo(location.primaryTerm()));
        assertThat(in.readString(), equalTo(location.blobName()));
        assertThat(in.readVLong(), equalTo(location.offset()));
        assertThat(in.readVLong(), equalTo(location.fileLength()));
        assertThat(in.available(), equalTo(0));

        assertThat(copyWriteable(location, writableRegistry(), BlobLocation::readFromTransport, oldVersion), equalTo(location));
    }

    /// A merge blob cannot be sent to a node that does not know about them.
    public void testMergeBlobCannotBeWrittenBeforeBlobSerialization() {
        var oldVersion = TransportVersionUtils.getPreviousVersion(BLOB_LOCATION_SERIALIZES_BLOB);
        var location = new BlobLocation(randomMergeBlob(), randomLongBetween(0, 100), randomLongBetween(1, 1000));

        var out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        expectThrows(IllegalStateException.class, () -> location.writeTo(out));
    }

    /// A malformed blob name received from a node of a previous version is reported as an [IOException].
    public void testReadMalformedBlobNameBeforeBlobSerialization() throws IOException {
        var oldVersion = TransportVersionUtils.getPreviousVersion(BLOB_LOCATION_SERIALIZES_BLOB);
        var out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        out.writeVLong(1L);
        out.writeString("not_a_blob_name");
        out.writeVLong(0L);
        out.writeVLong(1L);

        var in = out.bytes().streamInput();
        in.setTransportVersion(oldVersion);
        var exception = expectThrows(IOException.class, () -> BlobLocation.readFromTransport(in));
        assertThat(exception.getMessage(), containsString("malformed blob name [not_a_blob_name]"));
    }

    /// The XContent format is the same for both kinds of blob: the type is recognized by the prefix of the blob name.
    public void testXContentOfEachKindOfBlob() throws IOException {
        for (Blob blob : List.of(randomBccBlob(), randomMergeBlob())) {
            var location = new BlobLocation(blob, randomLongBetween(0, 100), randomLongBetween(1, 1000));
            var json = "{\"primary_term\":"
                + blob.primaryTerm()
                + ",\"blob_name\":\""
                + blob.blobName()
                + "\",\"offset\":"
                + location.offset()
                + ",\"file_length\":"
                + location.fileLength()
                + "}";

            assertThat(Strings.toString(location), equalTo(json));
            try (var parser = createParser(JsonXContent.jsonXContent, json)) {
                var parsed = BlobLocation.fromXContent(parser);
                assertThat(parsed, equalTo(location));
                assertThat(parsed.blob().getClass(), equalTo(blob.getClass()));
            }
        }
    }

    public void testXContentRejectsUnrecognizedBlobName() throws IOException {
        var json = "{\"primary_term\":1,\"blob_name\":\"not_a_blob_name\",\"offset\":0,\"file_length\":1}";
        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            expectThrows(XContentParseException.class, () -> BlobLocation.fromXContent(parser));
        }
    }

    public void testContains() {
        var bcc = randomBccBlob();
        var outer = new BlobLocation(bcc, 10, 100);
        assertTrue(outer.contains(new BlobLocation(bcc, 10, 100)));
        assertTrue(outer.contains(new BlobLocation(bcc, 20, 50)));
        assertFalse(outer.contains(new BlobLocation(bcc, 5, 10)));
        assertFalse(outer.contains(new BlobLocation(bcc, 100, 20)));

        var mergeBlob = randomMergeBlob();
        var mergeLocation = new BlobLocation(mergeBlob, 10, 100);
        assertTrue(mergeLocation.contains(new BlobLocation(mergeBlob, 20, 50)));
        // same range, but not in the same blob
        assertFalse(outer.contains(new BlobLocation(mergeBlob, 20, 50)));
        assertFalse(mergeLocation.contains(new BlobLocation(bcc, 20, 50)));
    }

    public void testAccessorsDelegateToBlob() {
        for (Blob blob : List.of(randomBccBlob(), randomMergeBlob())) {
            var location = new BlobLocation(blob, 0, 1);
            assertThat(location.primaryTerm(), equalTo(blob.primaryTerm()));
            assertThat(location.blobName(), equalTo(blob.blobName()));
        }
    }

    private static Blob randomBlob() {
        return randomBoolean() ? randomBccBlob() : randomMergeBlob();
    }

    private static BccBlob randomBccBlob() {
        return new BccBlob(BccBlob.blobNameFromGeneration(randomLongBetween(1, 1000)), randomLongBetween(1, 10));
    }

    private static MergeBlob randomMergeBlob() {
        return new MergeBlob(MergeBlob.blobName(randomByteArrayOfLength(StringHelper.ID_LENGTH)), randomLongBetween(1, 10));
    }
}
