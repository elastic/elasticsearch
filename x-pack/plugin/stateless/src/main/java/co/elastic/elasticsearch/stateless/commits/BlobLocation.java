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
 */

package co.elastic.elasticsearch.stateless.commits;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions.BLOB_LOCATION_WITHOUT_BLOB_LENGTH;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * References a location where a file is written on the blob store.
 *
 * @param blobFile the blob file in which this location points
 * @param blobLength the length of the blob file TODO: to be removed
 * @param offset the offset inside the blob file where the file is written
 * @param fileLength the length of the file
 */
public record BlobLocation(BlobFile blobFile, long blobLength, long offset, long fileLength) implements Writeable, ToXContentObject {

    public BlobLocation {
        assert blobLength == Long.MIN_VALUE || offset + fileLength <= blobLength
            : "(offset + file) length is greater than blobLength " + this;
    }

    public BlobLocation(long primaryTerm, String blobName, long blobLength, long offset, long fileLength) {
        this(new BlobFile(primaryTerm, blobName), blobLength, offset, fileLength);
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

    public static BlobLocation readFromStore(StreamInput streamInput, boolean includesBlobLength) throws IOException {
        if (includesBlobLength) {
            return readWithBlobLength(streamInput);
        } else {
            return readWithoutBlobLength(streamInput);
        }
    }

    public static BlobLocation readFromTransport(StreamInput streamInput) throws IOException {
        if (streamInput.getTransportVersion().before(BLOB_LOCATION_WITHOUT_BLOB_LENGTH)) {
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
        long blobLength = Long.MIN_VALUE;
        return new BlobLocation(primaryTerm, blobName, blobLength, offset, length);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(primaryTerm());
        out.writeString(blobName());
        if (out.getTransportVersion().before(BLOB_LOCATION_WITHOUT_BLOB_LENGTH)) {
            assert blobLength != Long.MIN_VALUE : "target node needs blobLength but this node does not have one";
            out.writeVLong(blobLength);
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
            // TODO: Remove parsing blobLength from object store. Need it for now since it can be resent as new notification to an old node
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

    // TODO: Remove the overridden equals method once blobLength is removed from the class
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlobLocation that = (BlobLocation) o;
        return offset == that.offset && fileLength == that.fileLength && Objects.equals(blobFile, that.blobFile);
    }

    // TODO: Remove the overridden hashCode method once blobLength is removed from the class
    @Override
    public int hashCode() {
        return Objects.hash(blobFile, offset, fileLength);
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
