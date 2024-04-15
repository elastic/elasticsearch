/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions.BLOB_LOCATION_WITHOUT_BLOB_LENGTH;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents a file embedded in a {@code BlobFile} stored in the blobstore.
 *
 * In order to save costs for uploading multiple files (such as compound commits)
 * to the blobstore they are merged into a single {@code BlobFile}.
 *
 * Each of the original files could be located in {@code BlobFile} using offset and fileLength.
 *
 * @param blobFile the blob file containing this location
 * @param offset the offset inside the blob file where the file is written
 * @param fileLength the length of the embedded file
 */
public record BlobLocation(BlobFile blobFile, long offset, long fileLength) implements Writeable, ToXContentObject {

    public BlobLocation {
        assert offset >= 0 : "offset " + offset + " < 0";
        assert fileLength > 0 : "fileLength " + fileLength + " <= 0";
    }

    public BlobLocation(long primaryTerm, String blobName, long offset, long fileLength) {
        this(new BlobFile(primaryTerm, blobName), offset, fileLength);
    }

    public long primaryTerm() {
        return blobFile.primaryTerm();
    }

    public String blobName() {
        return blobFile.blobName();
    }

    /**
     * @return parse the generation from the blob name
     */
    public long compoundFileGeneration() {
        return StatelessCompoundCommit.parseGenerationFromBlobName(blobName());
    }

    public PrimaryTermAndGeneration getBatchedCompoundCommitTermAndGeneration() {
        return new PrimaryTermAndGeneration(primaryTerm(), compoundFileGeneration());
    }

    /**
     * This method is used to read BlobLocation from the object store. Ancient commits (before xcontent) can still contain blobLength
     */
    public static BlobLocation readFromStore(StreamInput streamInput, boolean includesBlobLength) throws IOException {
        if (includesBlobLength) {
            return readWithBlobLength(streamInput);
        } else {
            return readWithoutBlobLength(streamInput);
        }
    }

    public static BlobLocation readFromTransport(StreamInput streamInput) throws IOException {
        // TODO: remove this BWC version check
        if (streamInput.getTransportVersion().before(BLOB_LOCATION_WITHOUT_BLOB_LENGTH)) {
            final String message = "remote node version too low " + streamInput.getTransportVersion();
            assert false : message;
            throw new IllegalStateException(message);
        } else {
            return readWithoutBlobLength(streamInput);
        }
    }

    private static BlobLocation readWithBlobLength(StreamInput streamInput) throws IOException {
        long primaryTerm = streamInput.readVLong();
        String blobName = streamInput.readString();
        streamInput.readVLong(); // ignore blobLength
        long offset = streamInput.readVLong();
        long length = streamInput.readVLong();
        return new BlobLocation(primaryTerm, blobName, offset, length);
    }

    private static BlobLocation readWithoutBlobLength(StreamInput streamInput) throws IOException {
        long primaryTerm = streamInput.readVLong();
        String blobName = streamInput.readString();
        long offset = streamInput.readVLong();
        long length = streamInput.readVLong();
        return new BlobLocation(primaryTerm, blobName, offset, length);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(primaryTerm());
        out.writeString(blobName());
        // TODO: remove this BWC version check
        if (out.getTransportVersion().before(BLOB_LOCATION_WITHOUT_BLOB_LENGTH)) {
            final String message = "remote node version too low " + out.getTransportVersion();
            assert false : message;
            throw new IllegalStateException(message);
        }
        out.writeVLong(offset);
        out.writeVLong(fileLength);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("primary_term", primaryTerm())
            .field("blob_name", blobName())
            // TODO: Remove writing blobLength to object store. Need it for now since an old node may read the commit
            .field("blob_length", Long.MIN_VALUE)
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
            return new BlobLocation(primaryTerm, blobName, offset, fileLength);
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
