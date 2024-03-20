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

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * References a location where a file is written on the blob store.
 *
 * @param blobFile the blob file in which this location points
 * @param offset the offset inside the blob file where the file is written
 * @param fileLength the length of the file
 */
public record BlobLocation(BlobFile blobFile, long offset, long fileLength) implements Writeable, ToXContentObject {

    public BlobLocation {
        assert offset + fileLength <= blobFile.blobLength() : "(offset + file) length is greater than blobLength " + this;
    }

    public BlobLocation(long primaryTerm, String blobName, long blobLength, long offset, long fileLength) {
        this(new BlobFile(primaryTerm, blobName, blobLength), offset, fileLength);
    }

    public long primaryTerm() {
        return blobFile.primaryTerm();
    }

    public String blobName() {
        return blobFile.blobName();
    }

    public long blobLength() {
        return blobFile.blobLength();
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
        return readWithBlobLength(streamInput);
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
        if (StatelessCompoundCommit.startsWithBlobPrefix(blobName)) {
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
        out.writeVLong(primaryTerm());
        out.writeString(blobName());
        if (includeBlobLength) {
            out.writeVLong(blobLength());
        } else {
            assert blobLength() == fileLength;
        }
        out.writeVLong(offset);
        out.writeVLong(fileLength);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(primaryTerm());
        out.writeString(blobName());
        out.writeVLong(blobLength());
        out.writeVLong(offset);
        out.writeVLong(fileLength);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("primary_term", primaryTerm())
            .field("blob_name", blobName())
            .field("blob_length", blobLength())
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
            + primaryTerm()
            + ", blobName='"
            + blobName()
            + "', blobLength="
            + blobLength()
            + ", offset="
            + offset
            + ", fileLength="
            + fileLength
            + '}';
    }
}
