/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.commits;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * References a location where a file is written on the blob store.
 *
 * @param primaryTerm the primary term where the file was first created
 * @param blobName the name of the blob in which the file is written
 * @param blobLength the length of the enclosing blob
 * @param offset the offset inside the blob where the file is written
 * @param fileLength the length of the file
 */
public record BlobLocation(long primaryTerm, String blobName, long blobLength, long offset, long fileLength)
    implements
        Writeable,
        ToXContentObject {

    public BlobLocation {
        assert offset + fileLength <= blobLength : "(offset + file) length is greater than blobLength " + this;
    }

    public static BlobLocation readFromStore(StreamInput streamInput, boolean includesBlobLength) throws IOException {
        if (includesBlobLength) {
            return readWithBlobLength(streamInput);
        } else {
            return readWithoutBlobLength(streamInput);
        }

    }

    public static BlobLocation readFromTransport(StreamInput streamInput) throws IOException {
        if (streamInput.getTransportVersion().onOrAfter(TransportVersion.V_8_500_008)) {
            return readWithBlobLength(streamInput);
        } else {
            return readWithoutBlobLength(streamInput);
        }
    }

    private static BlobLocation readWithBlobLength(StreamInput streamInput) throws IOException {
        return new BlobLocation(
            streamInput.readVLong(),
            streamInput.readString(),
            streamInput.readVLong(),
            streamInput.readVLong(),
            streamInput.readVLong()
        );
    }

    private static BlobLocation readWithoutBlobLength(StreamInput streamInput) throws IOException {
        long primaryTerm = streamInput.readVLong();
        String blobName = streamInput.readString();
        long offset = streamInput.readVLong();
        long length = streamInput.readVLong();
        long blobLength;
        if (blobName.startsWith(StatelessCompoundCommit.NAME)) {
            // Only the one segments file was stored in versions prior to the blobLength being added
            blobLength = offset + length;
        } else {
            // Non-segments files were not stored in combined blobs prior to the blobLength being added
            assert offset == 0;
            blobLength = length;
        }
        return new BlobLocation(primaryTerm, blobName, blobLength, offset, length);
    }

    public void writeToStore(StreamOutput out, boolean includeBlobLength) throws IOException {
        out.writeVLong(primaryTerm);
        out.writeString(blobName);
        if (includeBlobLength) {
            out.writeVLong(blobLength);
        } else {
            assert blobLength == fileLength;
        }
        out.writeVLong(offset);
        out.writeVLong(fileLength);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(primaryTerm);
        out.writeString(blobName);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_008)) {
            out.writeVLong(blobLength);
        }
        out.writeVLong(offset);
        out.writeVLong(fileLength);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("primary_term", primaryTerm)
            .field("blob_name", blobName)
            .field("blob_length", blobLength)
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
            long blobLength = (long) args[2];
            long offset = (long) args[3];
            long fileLength = (long) args[4];
            return new BlobLocation(primaryTerm, blobName, blobLength, offset, fileLength);
        }
    );
    static {
        PARSER.declareLong(constructorArg(), new ParseField("primary_term"));
        PARSER.declareString(constructorArg(), new ParseField("blob_name"));
        PARSER.declareLong(constructorArg(), new ParseField("blob_length"));
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
            + primaryTerm
            + ", blobName='"
            + blobName
            + '\''
            + ", blobLength="
            + blobLength
            + ", offset="
            + offset
            + ", fileLength="
            + fileLength
            + '}';
    }
}
