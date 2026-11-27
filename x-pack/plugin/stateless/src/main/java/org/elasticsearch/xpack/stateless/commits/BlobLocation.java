/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/// Represents a file embedded in a [Blob] stored in the blob store.
///
/// In order to save costs for uploading multiple files to the blob store, files are concatenated together into a single [Blob]. Then each
/// of the original file can be located in the [Blob] using an offset and a fileLength.
///
/// @param blob the blob file containing this location
/// @param offset the offset inside the blob file where the file is written
/// @param fileLength the length of the embedded file
public record BlobLocation(Blob blob, long offset, long fileLength) implements Writeable, ToXContentObject {

    /// From this version onwards, the blob is serialized on the transport layer with [Blob#writeTo] instead of a primary term followed by a
    /// blob name, so that any type of blobs can be sent.
    static final TransportVersion BLOB_LOCATION_SERIALIZES_BLOB = TransportVersion.fromName("stateless_blob_location_serializes_blob");

    public BlobLocation {
        assert offset >= 0 : "offset " + offset + " < 0";
        assert fileLength > 0 : "fileLength " + fileLength + " <= 0";
    }

    public long primaryTerm() {
        return blob.primaryTerm();
    }

    public String blobName() {
        return blob.blobName();
    }

    /// @return the blob containing this location as a [BccBlob], for the callers that only deal with batched compound commits
    /// @throws IllegalStateException if the blob is not a [BccBlob]
    @Deprecated(forRemoval = true)
    public BccBlob bccBlob() {
        if (blob instanceof BccBlob bccBlob) {
            return bccBlob;
        }
        final var e = new IllegalStateException("expected a BccBlob but blob [" + blob + "] is a " + blob.getClass().getSimpleName());
        assert false : e;
        throw e;
    }

    @Deprecated(forRemoval = true)
    public PrimaryTermAndGeneration getBatchedCompoundCommitTermAndGeneration() {
        return bccBlob().termAndGeneration();
    }

    /// Returns true if this BlobLocation fully contains the given BlobLocation.
    public boolean contains(BlobLocation blobLocation) {
        return Objects.equals(blobLocation.blob(), blob())
            && blobLocation.offset() >= offset()
            && blobLocation.offset() + blobLocation.fileLength() <= offset() + fileLength();
    }

    public static BlobLocation readFromTransport(StreamInput streamInput) throws IOException {
        if (streamInput.getTransportVersion().supports(BLOB_LOCATION_SERIALIZES_BLOB)) {
            Blob blob = Blob.readFrom(streamInput, Blob.class);
            long offset = streamInput.readVLong();
            long length = streamInput.readVLong();
            return new BlobLocation(blob, offset, length);
        }
        // nodes of previous versions only know about BccBlob
        long primaryTerm = streamInput.readVLong();
        String blobName = streamInput.readString();
        long offset = streamInput.readVLong();
        long length = streamInput.readVLong();
        try {
            return new BlobLocation(new BccBlob(blobName, primaryTerm), offset, length);
        } catch (IllegalArgumentException e) {
            throw new IOException("malformed blob name [" + blobName + ']', e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(BLOB_LOCATION_SERIALIZES_BLOB)) {
            blob.writeTo(out);
        } else {
            // nodes of previous versions only know about BccBlob
            if (blob instanceof BccBlob == false) {
                throw new IllegalStateException(
                    "cannot send blob [" + blob + "] to a node that only supports batched compound commit blobs"
                );
            }
            out.writeVLong(primaryTerm());
            out.writeString(blobName());
        }
        out.writeVLong(offset);
        out.writeVLong(fileLength);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("primary_term", primaryTerm())
            .field("blob_name", blobName())
            .field("offset", offset)
            .field("file_length", fileLength)
            .endObject();
    }

    private static final ConstructingObjectParser<BlobLocation, Void> PARSER = new ConstructingObjectParser<>(
        "blob_location",
        true,
        args -> {
            long primaryTerm = (long) args[0];
            String blobName = (String) args[1];
            long offset = (long) args[2];
            long fileLength = (long) args[3];

            return new BlobLocation(Blob.of(blobName, primaryTerm), offset, fileLength);
        }
    );
    static {
        PARSER.declareLong(constructorArg(), new ParseField("primary_term"));
        PARSER.declareString(constructorArg(), new ParseField("blob_name"));
        PARSER.declareLong(constructorArg(), new ParseField("offset"));
        PARSER.declareLong(constructorArg(), new ParseField("file_length"));
    }

    public static BlobLocation fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return "BlobLocation{"
            + "primaryTerm="
            + primaryTerm()
            + ", blobName='"
            + blobName()
            + ", offset="
            + offset
            + ", fileLength="
            + fileLength
            + '}';
    }
}
